// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::eraftpb::{ConfChangeSingle, ConfChangeType};
use crate::tracker::{Configuration, ProgressMap, ProgressTracker};
use crate::{Error, Result};

#[derive(Debug)]
/// Change log for progress map.
pub enum MapChangeType {
    Add,
    Remove,
}

/// Changes made by `Changer`.
pub type MapChange = Vec<(u64, MapChangeType)>;

#[derive(Debug)]
/// A map that stores updates instead of apply them directly.
pub struct IncrChangeMap<'a> {
    changes: MapChange,
    base: &'a ProgressMap,
}

impl IncrChangeMap<'_> {
    pub fn into_changes(self) -> MapChange {
        self.changes
    }

    fn contains(&self, id: u64) -> bool {
        match self.changes.iter().rfind(|(i, _)| *i == id) {
            Some((_, MapChangeType::Remove)) => false,
            Some((_, MapChangeType::Add)) => true,
            None => self.base.contains_key(&id),
        }
    }
}

#[derive(Debug)]
/// Changer facilitates configuration changes. It exposes methods to handle
/// simple and joint consensus while performing the proper validation that allows
/// refusing invalid configuration changes before they affect the active
/// configuration.
pub struct Changer<'a> {
    tracker: &'a ProgressTracker,
}

impl Changer<'_> {
    /// Creates a changer.
    pub fn new(tracker: &ProgressTracker) -> Changer {
        Changer { tracker }
    }

    /// Verifies that the outgoing (=right) majority config of the joint
    /// config is empty and initializes it with a copy of the incoming (=left)
    /// majority config. That is, it transitions from
    /// ```text
    ///     (1 2 3)&&()
    /// ```
    /// to
    /// ```text
    ///     (1 2 3)&&(1 2 3)
    /// ```.
    ///
    /// The supplied changes are then applied to the incoming majority config,
    /// resulting in a joint configuration that in terms of the Raft thesis[1]
    /// (Section 4.3) corresponds to `C_{new,old}`.
    ///
    /// [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
    pub fn enter_joint(
        &self,
        auto_leave: bool,
        ccs: &[ConfChangeSingle],
    ) -> Result<(Configuration, MapChange)> {
        if joint(self.tracker.conf()) {
            return Err(Error::ConfChangeError(
                "configuration is already joint".to_owned(),
            ));
        }
        let (mut cfg, mut prs) = self.check_and_copy()?;

        if cfg.voters().incoming.is_empty() {
            // We allow adding nodes to an empty config for convenience (testing and
            // bootstrap), but you can't enter a joint state.
            return Err(Error::ConfChangeError(
                "can't make a zero-voter config joint".to_owned(),
            ));
        }
        cfg.voters
            .outgoing
            .extend(cfg.voters.incoming.iter().cloned());
        self.apply(&mut cfg, &mut prs, ccs)?;
        cfg.auto_leave = auto_leave;
        check_invariants(&cfg, &prs)?;
        Ok((cfg, prs.into_changes()))
    }

    /// Transitions out of a joint configuration. It is an error to call this method if
    /// the configuration is not joint, i.e. if the outgoing majority config is empty.
    ///
    /// The outgoing majority config of the joint configuration will be removed, that is,
    /// the incoming config is promoted as the sole decision maker. In the notation of
    /// the Raft thesis[1] (Section 4.3), this method transitions from `C_{new,old}` into
    /// `C_new`.
    ///
    /// At the same time, any staged learners (LearnersNext) the addition of which was
    /// held back by an overlapping voter in the former outgoing config will be inserted
    /// into Learners.
    ///
    /// [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
    pub fn leave_joint(&self) -> Result<(Configuration, MapChange)> {
        if !joint(self.tracker.conf()) {
            return Err(Error::ConfChangeError(
                "can't leave a non-joint config".to_owned(),
            ));
        }
        let (mut cfg, mut prs) = self.check_and_copy()?;
        if cfg.voters().outgoing.is_empty() {
            return Err(Error::ConfChangeError(format!(
                "configuration is not joint: {:?}",
                cfg
            )));
        }
        cfg.learners.extend(cfg.learners_next.drain());

        for id in &*cfg.voters.outgoing {
            if !cfg.voters.incoming.contains(id) && !cfg.learners.contains(id) {
                prs.changes.push((*id, MapChangeType::Remove));
            }
        }

        cfg.voters.outgoing.clear();
        cfg.auto_leave = false;
        check_invariants(&cfg, &prs)?;
        Ok((cfg, prs.into_changes()))
    }

    /// Carries out a series of configuration changes that (in aggregate) mutates the
    /// incoming majority config Voters[0] by at most one. This method will return an
    /// error if that is not the case, if the resulting quorum is zero, or if the
    /// configuration is in a joint state (i.e. if there is an outgoing configuration).
    pub fn simple(&mut self, ccs: &[ConfChangeSingle]) -> Result<(Configuration, MapChange)> {
        if joint(self.tracker.conf()) {
            return Err(Error::ConfChangeError(
                "can't apply simple config change in joint config".to_owned(),
            ));
        }
        let (mut cfg, mut prs) = self.check_and_copy()?;
        self.apply(&mut cfg, &mut prs, ccs)?;
        if cfg
            .voters
            .incoming
            .symmetric_difference(&self.tracker.conf().voters.incoming)
            .count()
            > 1
        {
            return Err(Error::ConfChangeError(
                "more than one voter changed without entering joint config".to_owned(),
            ));
        }
        check_invariants(&cfg, &prs)?;
        Ok((cfg, prs.into_changes()))
    }

    /// Applies a change to the configuration. By convention, changes to voters are always
    /// made to the incoming majority config. Outgoing is either empty or preserves the
    /// outgoing majority configuration while in a joint state.
    fn apply(
        &self,
        cfg: &mut Configuration,
        prs: &mut IncrChangeMap,
        ccs: &[ConfChangeSingle],
    ) -> Result<()> {
        for cc in ccs {
            if cc.node_id == 0 {
                // Replaces the NodeID with zero if it decides (downstream of
                // raft) to not apply a change, so we have to have explicit code
                // here to ignore these.
                continue;
            }
            match cc.get_change_type() {
                ConfChangeType::AddNode => self.make_voter(cfg, prs, cc.node_id),
                ConfChangeType::AddLearnerNode => self.make_learner(cfg, prs, cc.node_id),
                ConfChangeType::RemoveNode => self.remove(cfg, prs, cc.node_id),
            }
        }
        if cfg.voters().incoming.is_empty() {
            return Err(Error::ConfChangeError("removed all voters".to_owned()));
        }
        Ok(())
    }

    /// Adds or promotes the given ID to be a voter in the incoming majority config.
    fn make_voter(&self, cfg: &mut Configuration, prs: &mut IncrChangeMap, id: u64) {
        if !prs.contains(id) {
            self.init_progress(cfg, prs, id, false);
            return;
        }

        cfg.voters.incoming.insert(id);
        cfg.learners.remove(&id);
        cfg.learners_next.remove(&id);
    }

    /// Makes the given ID a learner or stages it to be a learner once an active joint
    /// configuration is exited.
    ///
    /// The former happens when the peer is not a part of the outgoing config, in which
    /// case we either add a new learner or demote a voter in the incoming config.
    ///
    /// The latter case occurs when the configuration is joint and the peer is a voter
    /// in the outgoing config. In that case, we do not want to add the peer as a learner
    /// because then we'd have to track a peer as a voter and learner simultaneously.
    /// Instead, we add the learner to LearnersNext, so that it will be added to Learners
    /// the moment the outgoing config is removed by LeaveJoint().
    fn make_learner(&self, cfg: &mut Configuration, prs: &mut IncrChangeMap, id: u64) {
        if !prs.contains(id) {
            self.init_progress(cfg, prs, id, true);
            return;
        }

        if cfg.learners.contains(&id) {
            return;
        }

        cfg.voters.incoming.remove(&id);
        cfg.learners.remove(&id);
        cfg.learners_next.remove(&id);

        // Use LearnersNext if we can't add the learner to Learners directly, i.e.
        // if the peer is still tracked as a voter in the outgoing config. It will
        // be turned into a learner in LeaveJoint().
        //
        // Otherwise, add a regular learner right away.
        if cfg.voters().outgoing.contains(&id) {
            cfg.learners_next.insert(id);
        } else {
            cfg.learners.insert(id);
        }
    }

    /// Removes this peer as a voter or learner from the incoming config.
    fn remove(&self, cfg: &mut Configuration, prs: &mut IncrChangeMap, id: u64) {
        if !prs.contains(id) {
            return;
        }

        cfg.voters.incoming.remove(&id);
        cfg.learners.remove(&id);
        cfg.learners_next.remove(&id);

        // If the peer is still a voter in the outgoing config, keep the Progress.
        if !cfg.voters.outgoing.contains(&id) {
            prs.changes.push((id, MapChangeType::Remove));
        }
    }

    /// Initializes a new progress for the given node or learner.
    fn init_progress(
        &self,
        cfg: &mut Configuration,
        prs: &mut IncrChangeMap,
        id: u64,
        is_learner: bool,
    ) {
        if !is_learner {
            cfg.voters.incoming.insert(id);
        } else {
            cfg.learners.insert(id);
        }
        prs.changes.push((id, MapChangeType::Add));
    }

    /// Copies the tracker's config. It returns an error if checkInvariants does.
    ///
    /// Unlike Etcd, we don't copy progress as we don't need to mutate the `is_learner`
    /// flags. Additions and Removals should be done after everything is checked OK.
    fn check_and_copy(&self) -> Result<(Configuration, IncrChangeMap)> {
        let prs = IncrChangeMap {
            changes: vec![],
            base: self.tracker.progress(),
        };
        check_invariants(self.tracker.conf(), &prs)?;
        Ok((self.tracker.conf().clone(), prs))
    }
}

/// Makes sure that the config and progress are compatible with each other.
/// This is used to check both what the Changer is initialized with, as well
/// as what it returns.
fn check_invariants(cfg: &Configuration, prs: &IncrChangeMap) -> Result<()> {
    // NB: intentionally allow the empty config. In production we'll never see a
    // non-empty config (we prevent it from being created) but we will need to
    // be able to *create* an initial config, for example during bootstrap (or
    // during tests). Instead of having to hand-code this, we allow
    // transitioning from an empty config into any other legal and non-empty
    // config.
    for id in cfg.voters().ids().iter() {
        if !prs.contains(id) {
            return Err(Error::ConfChangeError(format!(
                "no progress for voter {}",
                id
            )));
        }
    }
    for id in &cfg.learners {
        if !prs.contains(*id) {
            return Err(Error::ConfChangeError(format!(
                "no progress for learner {}",
                id
            )));
        }
        // Conversely Learners and Voters doesn't intersect at all.
        if cfg.voters().outgoing.contains(id) {
            return Err(Error::ConfChangeError(format!(
                "{} is in learners and outgoing voters",
                id
            )));
        }
        if cfg.voters().incoming.contains(id) {
            return Err(Error::ConfChangeError(format!(
                "{} is in learners and incoming voters",
                id
            )));
        }
    }
    for id in &cfg.learners_next {
        if !prs.contains(*id) {
            return Err(Error::ConfChangeError(format!(
                "no progress for learner(next) {}",
                id
            )));
        }

        // Any staged learner was staged because it could not be directly added due
        // to a conflicting voter in the outgoing config.
        if !cfg.voters().outgoing.contains(id) {
            return Err(Error::ConfChangeError(format!(
                "{} is in learners_next and outgoing voters",
                id
            )));
        }
    }

    if !joint(cfg) {
        // Etcd enforces outgoing and learner_next to be nil map. But there is no nil
        // in rust. We just check empty for simplicity.
        if !cfg.learners_next().is_empty() {
            return Err(Error::ConfChangeError(
                "learners_next must be empty when not joint".to_owned(),
            ));
        }
        if cfg.auto_leave {
            return Err(Error::ConfChangeError(
                "auto_leave must be false when not joint".to_owned(),
            ));
        }
    }

    Ok(())
}

#[inline]
fn joint(cfg: &Configuration) -> bool {
    !cfg.voters().outgoing.is_empty()
}

#[cfg(test)]
mod test {
    use crate::errors::Error::ConfChangeError;
    use crate::tracker::Configuration;
    use crate::Result;
    use crate::{default_logger, MapChange, ProgressState};
    use crate::{Changer, ProgressTracker};
    use raft_proto::parse_conf_change;

    enum CommandType {
        Simple,
        EnterJointWithAutoLeave,
        EnterJoint,
        LeaveJoint,
    }

    struct Command {
        command_type: CommandType,
        tokens: String,
    }

    impl Command {
        fn new(command_type: CommandType, tokens: &str) -> Self {
            Self {
                command_type,
                tokens: String::from(tokens),
            }
        }
    }

    fn execute_commands(c: &mut Changer, command: Command) -> Result<(Configuration, MapChange)> {
        let ccs = match parse_conf_change(command.tokens.as_str()) {
            Ok(ccs) => ccs,
            Err(err) => panic!("{}", err),
        };
        match command.command_type {
            CommandType::Simple => c.simple(&ccs),
            CommandType::EnterJointWithAutoLeave => c.enter_joint(true, &ccs),
            CommandType::EnterJoint => c.enter_joint(false, &ccs),
            CommandType::LeaveJoint => c.leave_joint(),
        }
    }

    #[test]
    fn test_confchange() {
        let mut test_cases = vec![
            // joint autoleave
            vec![
                (
                    Command::new(CommandType::Simple, "v1"),
                    Configuration::new_conf(vec![1], vec![], vec![], vec![], false),
                    vec![(1, ProgressState::Probe, 0, 0)],
                    "",
                ),
                (
                    Command::new(CommandType::EnterJointWithAutoLeave, "v2 v3"),
                    Configuration::new_conf(vec![1, 2, 3], vec![1], vec![], vec![], true),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 1),
                        (3, ProgressState::Probe, 0, 1),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::EnterJoint, ""),
                    Configuration::default(),
                    vec![],
                    "configuration is already joint",
                ),
                (
                    Command::new(CommandType::LeaveJoint, ""),
                    Configuration::new_conf(vec![1, 2, 3], vec![], vec![], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 1),
                        (3, ProgressState::Probe, 0, 1),
                    ],
                    "",
                ),
            ],
            // joint_idempotency
            vec![
                (
                    Command::new(CommandType::Simple, "v1"),
                    Configuration::new_conf(vec![1], vec![], vec![], vec![], false),
                    vec![(1, ProgressState::Probe, 0, 0)],
                    "",
                ),
                (
                    Command::new(
                        CommandType::EnterJoint,
                        "r1 r2 r9 v2 v3 v4 v2 v3 v4 l2 l2 r4 r4 l1 l1",
                    ),
                    Configuration::new_conf(vec![3], vec![1], vec![2], vec![1], false),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 1),
                        (3, ProgressState::Probe, 0, 1),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::LeaveJoint, ""),
                    Configuration::new_conf(vec![3], vec![], vec![1, 2], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 1),
                        (3, ProgressState::Probe, 0, 1),
                    ],
                    "",
                ),
            ],
            // joint_learners_next
            vec![
                (
                    Command::new(CommandType::Simple, "v1"),
                    Configuration::new_conf(vec![1], vec![], vec![], vec![], false),
                    vec![(1, ProgressState::Probe, 0, 0)],
                    "",
                ),
                (
                    Command::new(CommandType::EnterJoint, "v2 l1"),
                    Configuration::new_conf(vec![2], vec![1], vec![], vec![1], false),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 1),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::LeaveJoint, ""),
                    Configuration::new_conf(vec![2], vec![], vec![1], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 1),
                    ],
                    "",
                ),
            ],
            // joint_safety
            vec![
                (
                    Command::new(CommandType::LeaveJoint, ""),
                    Configuration::default(),
                    vec![],
                    "can\'t leave a non-joint config",
                ),
                (
                    Command::new(CommandType::EnterJoint, "v1"),
                    Configuration::default(),
                    vec![],
                    "can\'t make a zero-voter config joint",
                ),
                (
                    Command::new(CommandType::EnterJoint, "v1"),
                    Configuration::default(),
                    vec![],
                    "can\'t make a zero-voter config joint",
                ),
                (
                    Command::new(CommandType::Simple, "v1"),
                    Configuration::new_conf(vec![1], vec![], vec![], vec![], false),
                    vec![(1, ProgressState::Probe, 0, 3)],
                    "",
                ),
                (
                    Command::new(CommandType::LeaveJoint, ""),
                    Configuration::default(),
                    vec![],
                    "can\'t leave a non-joint config",
                ),
                (
                    Command::new(CommandType::EnterJoint, ""),
                    Configuration::new_conf(vec![1], vec![1], vec![], vec![], false),
                    vec![(1, ProgressState::Probe, 0, 3)],
                    "",
                ),
                (
                    Command::new(CommandType::EnterJoint, ""),
                    Configuration::default(),
                    vec![],
                    "configuration is already joint",
                ),
                (
                    Command::new(CommandType::LeaveJoint, ""),
                    Configuration::new_conf(vec![1], vec![], vec![], vec![], false),
                    vec![(1, ProgressState::Probe, 0, 3)],
                    "",
                ),
                (
                    Command::new(CommandType::LeaveJoint, ""),
                    Configuration::default(),
                    vec![],
                    "can\'t leave a non-joint config",
                ),
                (
                    Command::new(CommandType::EnterJoint, "r1 v2 v3 l4"),
                    Configuration::new_conf(vec![2, 3], vec![1], vec![4], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 3),
                        (2, ProgressState::Probe, 0, 9),
                        (3, ProgressState::Probe, 0, 9),
                        (4, ProgressState::Probe, 0, 9),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::EnterJoint, ""),
                    Configuration::default(),
                    vec![],
                    "configuration is already joint",
                ),
                (
                    Command::new(CommandType::EnterJoint, "v12"),
                    Configuration::default(),
                    vec![],
                    "configuration is already joint",
                ),
                (
                    Command::new(CommandType::Simple, "l15"),
                    Configuration::default(),
                    vec![],
                    "can\'t apply simple config change in joint config",
                ),
                (
                    Command::new(CommandType::LeaveJoint, ""),
                    Configuration::new_conf(vec![2, 3], vec![], vec![4], vec![], false),
                    vec![
                        (2, ProgressState::Probe, 0, 9),
                        (3, ProgressState::Probe, 0, 9),
                        (4, ProgressState::Probe, 0, 9),
                    ],
                    "can\'t leave a non-joint config",
                ),
                (
                    Command::new(CommandType::Simple, "l9"),
                    Configuration::new_conf(vec![2, 3], vec![], vec![4, 9], vec![], false),
                    vec![
                        (2, ProgressState::Probe, 0, 9),
                        (3, ProgressState::Probe, 0, 9),
                        (4, ProgressState::Probe, 0, 9),
                        (9, ProgressState::Probe, 0, 14),
                    ],
                    "",
                ),
            ],
            // simple_idempotency
            vec![
                (
                    Command::new(CommandType::Simple, "v1"),
                    Configuration::new_conf(vec![1], vec![], vec![], vec![], false),
                    vec![(1, ProgressState::Probe, 0, 0)],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "v1"),
                    Configuration::new_conf(vec![1], vec![], vec![], vec![], false),
                    vec![(1, ProgressState::Probe, 0, 0)],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "v2"),
                    Configuration::new_conf(vec![1, 2], vec![], vec![], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 2),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "l1"),
                    Configuration::new_conf(vec![2], vec![], vec![1], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 2),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "l1"),
                    Configuration::new_conf(vec![2], vec![], vec![1], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 2),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "r1"),
                    Configuration::new_conf(vec![2], vec![], vec![], vec![], false),
                    vec![(2, ProgressState::Probe, 0, 2)],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "r1"),
                    Configuration::new_conf(vec![2], vec![], vec![], vec![], false),
                    vec![(2, ProgressState::Probe, 0, 2)],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "v3"),
                    Configuration::new_conf(vec![2, 3], vec![], vec![], vec![], false),
                    vec![
                        (2, ProgressState::Probe, 0, 2),
                        (3, ProgressState::Probe, 0, 7),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "r3"),
                    Configuration::new_conf(vec![2], vec![], vec![], vec![], false),
                    vec![(2, ProgressState::Probe, 0, 2)],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "r3"),
                    Configuration::new_conf(vec![2], vec![], vec![], vec![], false),
                    vec![(2, ProgressState::Probe, 0, 2)],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "r4"),
                    Configuration::new_conf(vec![2], vec![], vec![], vec![], false),
                    vec![(2, ProgressState::Probe, 0, 2)],
                    "",
                ),
            ],
            // simple_promote_demote
            vec![
                (
                    Command::new(CommandType::Simple, "v1"),
                    Configuration::new_conf(vec![1], vec![], vec![], vec![], false),
                    vec![(1, ProgressState::Probe, 0, 0)],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "v2"),
                    Configuration::new_conf(vec![1, 2], vec![], vec![], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 1),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "v3"),
                    Configuration::new_conf(vec![1, 2, 3], vec![], vec![], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 1),
                        (3, ProgressState::Probe, 0, 2),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "l1 v1"),
                    Configuration::new_conf(vec![1, 2, 3], vec![], vec![], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 1),
                        (3, ProgressState::Probe, 0, 2),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "l2"),
                    Configuration::new_conf(vec![1, 3], vec![], vec![2], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 1),
                        (3, ProgressState::Probe, 0, 2),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "v2 l2"),
                    Configuration::new_conf(vec![1, 3], vec![], vec![2], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 1),
                        (3, ProgressState::Probe, 0, 2),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "v2"),
                    Configuration::new_conf(vec![1, 2, 3], vec![], vec![], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 0),
                        (2, ProgressState::Probe, 0, 1),
                        (3, ProgressState::Probe, 0, 2),
                    ],
                    "",
                ),
            ],
            // simple safety
            vec![
                (
                    Command::new(CommandType::Simple, "l1"),
                    Configuration::default(),
                    vec![],
                    "removed all voters",
                ),
                (
                    Command::new(CommandType::Simple, "v1"),
                    Configuration::new_conf(vec![1], vec![], vec![], vec![], false),
                    vec![(1, ProgressState::Probe, 0, 1)],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "v2 l3"),
                    Configuration::new_conf(vec![1, 2], vec![], vec![3], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 1),
                        (2, ProgressState::Probe, 0, 2),
                        (3, ProgressState::Probe, 0, 2),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "r1 v5"),
                    Configuration::default(),
                    vec![],
                    "more than one voter changed without entering joint config",
                ),
                (
                    Command::new(CommandType::Simple, "r1 r2"),
                    Configuration::default(),
                    vec![],
                    "removed all voters",
                ),
                (
                    Command::new(CommandType::Simple, "v3 v4"),
                    Configuration::default(),
                    vec![],
                    "more than one voter changed without entering joint config",
                ),
                (
                    Command::new(CommandType::Simple, "l1 v5"),
                    Configuration::default(),
                    vec![],
                    "more than one voter changed without entering joint config",
                ),
                (
                    Command::new(CommandType::Simple, "l1 l2"),
                    Configuration::default(),
                    vec![],
                    "removed all voters",
                ),
                (
                    Command::new(CommandType::Simple, "l2 l3 l4 l5"),
                    Configuration::new_conf(vec![1], vec![], vec![2, 3, 4, 5], vec![], false),
                    vec![
                        (1, ProgressState::Probe, 0, 1),
                        (2, ProgressState::Probe, 0, 2),
                        (3, ProgressState::Probe, 0, 2),
                        (4, ProgressState::Probe, 0, 8),
                        (5, ProgressState::Probe, 0, 8),
                    ],
                    "",
                ),
                (
                    Command::new(CommandType::Simple, "r1"),
                    Configuration::default(),
                    vec![],
                    "removed all voters",
                ),
                (
                    Command::new(CommandType::Simple, "r2 r3 r4 r5"),
                    Configuration::new_conf(vec![1], vec![], vec![], vec![], false),
                    vec![(1, ProgressState::Probe, 0, 1)],
                    "",
                ),
            ],
            // id 0 doesn't change any
            vec![(
                Command::new(CommandType::Simple, "v1 v0"),
                Configuration::new_conf(vec![1], vec![], vec![], vec![], false),
                vec![(1, ProgressState::Probe, 0, 0)],
                "",
            )],
        ];

        for (test_case, mut commands) in test_cases.drain(..).enumerate() {
            let mut tr = ProgressTracker::new(10, default_logger());
            let mut c = Changer::new(&tr);
            for (index, (command, expected_conf, expected_prs, expected_err)) in
                commands.drain(..).enumerate()
            {
                match execute_commands(&mut c, command) {
                    Ok((conf, changes)) => {
                        tr.apply_conf(conf, changes, index as u64);
                        c = Changer::new(&tr);

                        let conf: Configuration = c.tracker.conf().clone();
                        let pr_map = c.tracker.progress();

                        let mut prs: Vec<_> = pr_map
                            .iter()
                            .map(|(&id, pr)| (id, pr.state, pr.matched, pr.next_idx))
                            .collect();
                        prs.sort_by(|a, b| a.0.cmp(&b.0));

                        assert_eq!(
                            (expected_conf, expected_prs),
                            (conf, prs),
                            "[test_case #{}, command_index #{}] mismatched result",
                            test_case + 1,
                            index + 1,
                        );
                    }
                    Err(e) => {
                        assert_eq!(
                            ConfChangeError(String::from(expected_err)),
                            e,
                            "[test_case #{}, command_index #{}] mismatched error",
                            test_case + 1,
                            index + 1,
                        );
                    }
                }
            }
        }
    }
}
