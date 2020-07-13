// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{AckedIndexer, Index, VoteResult};
use crate::{DefaultHashBuilder, HashSet};
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::{cmp, slice, u64};

/// A set of IDs that uses majority quorums to make decisions.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Configuration {
    voters: HashSet<u64>,
}

impl Configuration {
    /// Creates a new configuration using the given IDs.
    pub fn new(voters: HashSet<u64>) -> Configuration {
        Configuration { voters }
    }

    /// Creates an empty configuration with given capacity.
    pub fn with_capacity(cap: usize) -> Configuration {
        Configuration {
            voters: HashSet::with_capacity_and_hasher(cap, DefaultHashBuilder::default()),
        }
    }

    /// Returns the MajorityConfig as a sorted slice.
    pub fn slice(&self) -> Vec<u64> {
        let mut voters = self.raw_slice();
        voters.sort();
        voters
    }

    /// Returns the MajorityConfig as a slice.
    pub fn raw_slice(&self) -> Vec<u64> {
        self.voters.iter().cloned().collect()
    }

    /// Computes the committed index from those supplied via the
    /// provided AckedIndexer (for the active config).
    ///
    /// The bool flag indicates whether the index is computed by group commit algorithm
    /// successfully.
    ///
    /// Eg. If the matched indexes are [2,2,2,4,5], it will return 2.
    /// If the matched indexes and groups are `[(1, 1), (2, 2), (3, 2)]`, it will return 1.
    pub fn committed_index(&self, use_group_commit: bool, l: &impl AckedIndexer) -> (u64, bool) {
        if self.voters.is_empty() {
            // This plays well with joint quorums which, when one half is the zero
            // MajorityConfig, should behave like the other half.
            return (u64::MAX, true);
        }

        let mut stack_arr: [MaybeUninit<Index>; 7] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut heap_arr;
        let matched = if self.voters.len() <= 7 {
            for (i, v) in self.voters.iter().enumerate() {
                stack_arr[i] = MaybeUninit::new(l.acked_index(*v).unwrap_or_default());
            }
            unsafe {
                slice::from_raw_parts_mut(stack_arr.as_mut_ptr() as *mut _, self.voters.len())
            }
        } else {
            let mut buf = Vec::with_capacity(self.voters.len());
            for v in &self.voters {
                buf.push(l.acked_index(*v).unwrap_or_default());
            }
            heap_arr = Some(buf);
            heap_arr.as_mut().unwrap().as_mut_slice()
        };
        // Reverse sort.
        matched.sort_by(|a, b| b.index.cmp(&a.index));

        let quorum = crate::majority(matched.len());
        let quorum_index = matched[quorum - 1];
        if !use_group_commit {
            return (quorum_index.index, false);
        }
        let (quorum_commit_index, mut checked_group_id) =
            (quorum_index.index, quorum_index.group_id);
        let mut single_group = true;
        for m in matched.iter() {
            if m.group_id == 0 {
                single_group = false;
                continue;
            }
            if checked_group_id == 0 {
                checked_group_id = m.group_id;
                continue;
            }
            if checked_group_id == m.group_id {
                continue;
            }
            return (cmp::min(m.index, quorum_commit_index), true);
        }
        if single_group {
            (quorum_commit_index, false)
        } else {
            (matched.last().unwrap().index, false)
        }
    }

    /// Takes a mapping of voters to yes/no (true/false) votes and returns
    /// a result indicating whether the vote is pending (i.e. neither a quorum of
    /// yes/no has been reached), won (a quorum of yes has been reached), or lost (a
    /// quorum of no has been reached).
    pub fn vote_result(&self, check: impl Fn(u64) -> Option<bool>) -> VoteResult {
        if self.voters.is_empty() {
            // By convention, the elections on an empty config win. This comes in
            // handy with joint quorums because it'll make a half-populated joint
            // quorum behave like a majority quorum.
            return VoteResult::Won;
        }

        let (mut yes, mut missing) = (0, 0);
        for v in &self.voters {
            match check(*v) {
                Some(true) => yes += 1,
                None => missing += 1,
                _ => (),
            }
        }
        let q = crate::majority(self.voters.len());
        if yes >= q {
            VoteResult::Won
        } else if yes + missing >= q {
            VoteResult::Pending
        } else {
            VoteResult::Lost
        }
    }
}

impl Deref for Configuration {
    type Target = HashSet<u64>;

    #[inline]
    fn deref(&self) -> &HashSet<u64> {
        &self.voters
    }
}

impl DerefMut for Configuration {
    #[inline]
    fn deref_mut(&mut self) -> &mut HashSet<u64> {
        &mut self.voters
    }
}

#[cfg(test)]
mod test {
    use crate::{
        majority, AckIndexer, HashMap, HashSet, Index, JointConfig, MajorityConfig, VoteResult,
    };

    #[test]
    fn test_majority_commit_single_group() {
        let mut test_cases = vec![
            // [1] The empty quorum commits "everything". This is useful for its use in joint quorums.
            (vec![], vec![], u64::MAX),
            // [2] A single voter quorum is not final when no index is known.
            (vec![1], vec![0], 0),
            // [3] When an index is known, that's the committed index, and that's final.
            (vec![1], vec![12], 12),
            // [4] With two nodes, start out similarly.
            (vec![1, 2], vec![0, 0], 0),
            // [5]  The first committed index becomes known (for n1). Nothing changes in the
            // output because idx=12 is not known to be on a quorum (which is both nodes).
            (vec![1, 2], vec![12, 0], 0),
            // [6] The second index comes in and finalize the decision. The result will be the
            // smaller of the two indexes.
            (vec![1, 2], vec![12, 5], 5),
            // [7] 3 nodes
            (vec![1, 2, 3], vec![0, 0, 0], 0),
            (vec![1, 2, 3], vec![12, 0, 0], 0),
            (vec![1, 2, 3], vec![12, 5, 0], 5),
            (vec![1, 2, 3], vec![12, 5, 6], 6),
            //
            // test_cases: 11
            (vec![1, 2, 3], vec![12, 5, 4], 5),
            (vec![1, 2, 3], vec![5, 5, 0], 5),
            (vec![1, 2, 3], vec![5, 5, 12], 5),
            (vec![1, 2, 3], vec![100, 101, 103], 101),
            // [8] 5 nodes
            (vec![1, 2, 3, 4, 5], vec![0, 101, 103, 103, 104], 103),
            (vec![1, 2, 3, 4, 5], vec![101, 102, 103, 103, 0], 102),
        ];
        for (tc, (cfg, idx, expected_index)) in test_cases.drain(..).enumerate() {
            let cfg_set: HashSet<_> = cfg.iter().cloned().collect::<HashSet<_>>();
            let c = MajorityConfig::new(cfg_set.clone());

            let mut voters = vec![];
            voters.extend(cfg_set.into_iter());
            voters.sort();

            assert_eq!(
                voters.len(),
                idx.len(),
                "[test_cases #{}] error: mismatched input for voters {:?}: {:?}, check out test_cases",
                tc + 1,
                voters,
                idx
            );

            let mut l: AckIndexer = AckIndexer::default();

            for (i, &id) in voters.iter().enumerate() {
                l.insert(
                    id,
                    Index {
                        index: idx[i],
                        group_id: 0,
                    },
                );
            }

            let (index1, _) = c.clone().committed_index(false, &l);

            // These alternative computations should return the same
            // result. If not, print to the output.
            let index2 = if voters.is_empty() {
                u64::MAX
            } else {
                let mut index = 0;
                // calculate # of index greater than or equal to
                let mut nums = HashMap::default();

                // create a unique index vec
                let idx_set: HashSet<_> = idx.iter().cloned().collect::<HashSet<_>>();
                let mut idx_vec = vec![];
                idx_vec.extend(idx_set.into_iter());

                for x in idx_vec.iter().cloned() {
                    for y in idx.iter().cloned() {
                        if y >= x {
                            let counter = nums.entry(x).or_insert(0);
                            *counter += 1;
                        }
                    }
                }
                let quorum = majority(voters.len());

                for (&k, &v) in nums.iter() {
                    if v >= quorum && k > index {
                        index = k;
                    }
                }
                index
            };

            assert_eq!(
                index1,
                index2,
                "[test_cases #{}] alternative computation fails",
                tc + 1
            );

            // Joining a majority with the empty majority should give same result.
            let cc = JointConfig::new_joint(c.clone(), MajorityConfig::default());
            let index2 = cc.committed_index(false, &l).0;
            assert_eq!(
                index1,
                index2,
                "[test_cases #{}] zero-joint quorum fails",
                tc + 1
            );

            // Joining a majority with itself should give same result.
            let cc = JointConfig::new_joint(c.clone(), c.clone());
            let index2 = cc.committed_index(false, &l).0;
            assert_eq!(
                index1,
                index2,
                "[test_cases #{}] self-joint quorum fails",
                tc + 1
            );

            // overlaying
            // If the committed index was definitely above the currently inspected idx,
            // the result shouldn't change if we lower it further
            for (i, &id) in voters.iter().enumerate() {
                if idx[i] < index1 && idx[i] > 0 {
                    // case1: set idx[i] => idx[i] - 1
                    l.insert(
                        id,
                        Index {
                            index: idx[i] - 1,
                            group_id: 0,
                        },
                    );

                    let index2 = c.clone().committed_index(false, &l).0;
                    assert_eq!(
                        index1,
                        index2,
                        "[test_cases #{}] overlaying case 1 fails",
                        tc + 1
                    );

                    // case2: set idx[i] => 0

                    l.insert(
                        id,
                        Index {
                            index: 0,
                            group_id: 0,
                        },
                    );

                    let index2 = c.clone().committed_index(false, &l).0;
                    assert_eq!(
                        index1,
                        index2,
                        "[test_cases #{}] overlaying case 2 fails",
                        tc + 1
                    );

                    // recover
                    l.insert(
                        id,
                        Index {
                            index: idx[i],
                            group_id: 0,
                        },
                    );
                }
            }

            assert_eq!(
                index1,
                expected_index,
                "[test_cases #{}] index does not match expected value",
                tc + 1
            )
        }
    }

    #[test]
    fn test_majority_vote() {
        let mut test_cases = vec![
            // votes 0 => vote missing, 1 => vote no, 2 => vote yes

            // [1] The empty config always announces a won vote.
            (vec![], vec![], VoteResult::Won),
            (vec![1], vec![0], VoteResult::Pending),
            (vec![1], vec![1], VoteResult::Lost),
            (vec![123], vec![2], VoteResult::Won),
            (vec![4, 8], vec![0, 0], VoteResult::Pending),
            // [2] With two voters, a single rejection loses the vote.
            (vec![4, 8], vec![1, 0], VoteResult::Lost),
            (vec![4, 8], vec![2, 0], VoteResult::Pending),
            (vec![4, 8], vec![1, 2], VoteResult::Lost),
            (vec![4, 8], vec![2, 2], VoteResult::Won),
            (vec![2, 4, 7], vec![0; 3], VoteResult::Pending),
            //
            // testcases: 11
            (vec![2, 4, 7], vec![1, 0, 0], VoteResult::Pending),
            (vec![2, 4, 7], vec![2, 0, 0], VoteResult::Pending),
            (vec![2, 4, 7], vec![1, 1, 0], VoteResult::Lost),
            (vec![2, 4, 7], vec![1, 2, 0], VoteResult::Pending),
            (vec![2, 4, 7], vec![2, 2, 0], VoteResult::Won),
            (vec![2, 4, 7], vec![2, 2, 1], VoteResult::Won),
            (vec![2, 4, 7], vec![1, 2, 1], VoteResult::Lost),
            // [3] 7 nodes
            (
                vec![1, 2, 3, 4, 5, 6, 7],
                vec![2, 2, 1, 2, 0, 0, 0],
                VoteResult::Pending,
            ),
            (
                vec![1, 2, 3, 4, 5, 6, 7],
                vec![0, 2, 2, 0, 1, 2, 1],
                VoteResult::Pending,
            ),
            (
                vec![1, 2, 3, 4, 5, 6, 7],
                vec![2, 2, 1, 2, 0, 1, 2],
                VoteResult::Won,
            ),
            (
                vec![1, 2, 3, 4, 5, 6, 7],
                vec![2, 2, 0, 1, 2, 1, 1],
                VoteResult::Pending,
            ),
            (
                vec![1, 2, 3, 4, 5, 6, 7],
                vec![2, 2, 1, 2, 1, 1, 1],
                VoteResult::Lost,
            ),
        ];
        for (i, (cfg, votes, expected_vote_result)) in test_cases.drain(..).enumerate() {
            let cfg_set: HashSet<_> = cfg.iter().cloned().collect::<HashSet<_>>();

            let c = MajorityConfig::new(cfg_set.clone());

            let mut voters = vec![];
            voters.extend(cfg_set.into_iter());
            voters.sort();

            assert_eq!(
                voters.len(),
                votes.len(),
                "[test_cases #{}] error: mismatched input for voters {:?}: {:?}, check out test_cases",
                i + 1,
                voters,
                votes
            );

            let mut l: HashMap<u64, bool> = HashMap::default();

            for (i, id) in voters.drain(..).enumerate() {
                match votes[i] {
                    2 => l.insert(id, true),
                    1 => l.insert(id, false),
                    _ => None,
                };
            }

            let vote_result = c.vote_result(|id| l.get(&id).cloned());
            assert_eq!(
                vote_result,
                expected_vote_result,
                "[test_cases #{}] vote_result does not match expected value",
                i + 1
            )
        }
    }

    #[test]
    fn test_majority_commit_multi_group() {
        let mut test_cases = vec![
            // [1] The empty quorum commits "everything". This is useful for its use in joint quorums.
            (vec![], vec![], vec![], u64::MAX, true),
            // [2] A single voter quorum is not final when no index is known.
            (vec![1], vec![0], vec![0], 0, false),
            (vec![1], vec![0], vec![1], 0, false),
            // [3] When an index is known, that's the committed index, and that's final.
            (vec![1], vec![2], vec![1], 2, false),
            // [4] With two nodes, start out similarly.
            (vec![1, 2], vec![1, 1], vec![1, 1], 1, false),
            (vec![1, 2], vec![2, 3], vec![1, 1], 2, false),
            (vec![1, 2], vec![2, 3], vec![1, 2], 2, true),
            // [5] 3 nodes
            (vec![1, 2, 3], vec![2, 3, 4], vec![1, 1, 1], 3, false),
            (vec![1, 2, 3], vec![2, 3, 4], vec![1, 1, 0], 2, false),
            (vec![1, 2, 3], vec![2, 3, 4], vec![2, 1, 1], 2, true),
            (vec![1, 2, 3], vec![2, 3, 4], vec![2, 2, 1], 3, true),
            //
            // test_cases: 11
            (vec![1, 2, 3], vec![2, 3, 4], vec![2, 2, 0], 2, false),
            // [6] 5 nodes
            (
                vec![1, 2, 3, 4, 5],
                vec![2, 3, 4, 22, 33],
                vec![1, 1, 1, 1, 1],
                4,
                false,
            ),
            (
                vec![1, 2, 3, 4, 5],
                vec![2, 3, 4, 22, 33],
                vec![1, 1, 2, 1, 1],
                4,
                true,
            ),
            (
                vec![1, 2, 3, 4, 5],
                vec![2, 3, 4, 22, 33],
                vec![1, 1, 1, 2, 1],
                4,
                true,
            ),
            (
                vec![1, 2, 3, 4, 5],
                vec![2, 3, 4, 22, 33],
                vec![1, 2, 1, 2, 1],
                4,
                true,
            ),
            (
                vec![1, 2, 3, 4, 5],
                vec![2, 3, 4, 22, 33],
                vec![1, 2, 1, 0, 1],
                3,
                true,
            ),
            (
                vec![1, 2, 3, 4, 5],
                vec![2, 3, 4, 22, 33],
                vec![1, 0, 1, 0, 1],
                2,
                false,
            ),
        ];
        for (tc, (cfg, idx, group_ids, expected_index, expected_use_group_commit)) in
            test_cases.drain(..).enumerate()
        {
            let cfg_set: HashSet<_> = cfg.iter().cloned().collect::<HashSet<_>>();
            let c = MajorityConfig::new(cfg_set.clone());

            let mut voters = vec![];
            voters.extend(cfg_set.into_iter());
            voters.sort();

            assert_eq!(
                voters.len(),
                idx.len(),
                "[test_cases #{}] error: mismatched input for voters {:?}: {:?}, check out test_cases",
                tc + 1,
                voters,
                idx
            );

            let mut l: AckIndexer = AckIndexer::default();

            for (i, &id) in voters.iter().enumerate() {
                l.insert(
                    id,
                    Index {
                        index: idx[i],
                        group_id: group_ids[i],
                    },
                );
            }

            let (index1, use_group_commit1) = c.clone().committed_index(true, &l);

            // Joining a majority with the empty majority should give same result.
            let cc = JointConfig::new_joint(c.clone(), MajorityConfig::default());
            let (index2, use_group_commit2) = cc.committed_index(true, &l);
            assert_eq!(
                index1,
                index2,
                "[test_cases #{}] zero-joint quorum fails",
                tc + 1
            );
            assert_eq!(
                use_group_commit1,
                use_group_commit2,
                "[test_cases #{}] zero-joint quorum fails",
                tc + 1
            );

            // Joining a majority with itself should give same result.
            let cc = JointConfig::new_joint(c.clone(), c.clone());
            let (index2, use_group_commit2) = cc.committed_index(true, &l);
            assert_eq!(
                index1,
                index2,
                "[test_cases #{}] self-joint quorum fails",
                tc + 1
            );
            assert_eq!(
                use_group_commit1,
                use_group_commit2,
                "[test_cases #{}] self-joint quorum fails",
                tc + 1
            );

            // overlaying
            // If the committed index was definitely above the currently inspected idx,
            // the result shouldn't change if we lower it further
            for (i, &id) in voters.iter().enumerate() {
                if idx[i] < index1 && idx[i] > 0 {
                    // case1: set idx[i] => idx[i] - 1
                    l.insert(
                        id,
                        Index {
                            index: idx[i] - 1,
                            group_id: group_ids[i],
                        },
                    );

                    let index2 = c.clone().committed_index(true, &l).0;
                    assert_eq!(
                        index1,
                        index2,
                        "[test_cases #{}] overlaying case 1 fails",
                        tc + 1
                    );

                    // case2: set idx[i] => 0

                    l.insert(
                        id,
                        Index {
                            index: 0,
                            group_id: group_ids[i],
                        },
                    );

                    let index2 = c.clone().committed_index(true, &l).0;
                    assert_eq!(
                        index1,
                        index2,
                        "[test_cases #{}] overlaying case 2 fails",
                        tc + 1
                    );

                    // recover
                    l.insert(
                        id,
                        Index {
                            index: idx[i],
                            group_id: group_ids[i],
                        },
                    );
                }
            }

            assert_eq!(
                index1,
                expected_index,
                "[test_cases #{}] index does not match expected value",
                tc + 1
            );
            assert_eq!(
                use_group_commit1,
                expected_use_group_commit,
                "[test_cases #{}] index is not computed by group commit",
                tc + 1
            );
        }
    }
}
