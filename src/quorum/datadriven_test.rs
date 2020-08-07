use crate::quorum::{AckIndexer, Index, VoteResult};
use crate::{default_logger, HashMap, HashSet, JointConfig, MajorityConfig};
use anyhow::Result;
use datadriven::{run_test, TestData};

#[test]
fn test_data_driven_quorum() -> Result<()> {
    let logger = default_logger();

    fn test_quorum(data: &TestData) -> String {
        let mut ids: Vec<u64> = Vec::new();
        let mut idsj: Vec<u64> = Vec::new();
        let mut idxs: Vec<u64> = Vec::new();
        let mut groups: Vec<u64> = Vec::new();
        let mut votes: Vec<VoteResult> = Vec::new();

        let mut joint = false;

        for arg in &data.cmd_args {
            for val in &arg.vals {
                let val = val.as_str();

                match arg.key.as_str() {
                    "cfg" => {
                        let n = val.parse::<u64>().expect("n should be u64");
                        ids.push(n);
                    }
                    "cfgj" => {
                        joint = true;

                        if val == "zero" {
                            assert_eq!(arg.vals.len(), 1, "cannot mix 'zero' into configuration")
                        } else {
                            let n = val.parse::<u64>().expect("n should be u64");
                            idsj.push(n);
                        }
                    }
                    "idx" => {
                        let mut n = 0;
                        if val != "_" {
                            n = val.parse::<u64>().expect("n should be u64");
                            if n == 0 {
                                panic!("use '_' as 0")
                            }
                        }
                        idxs.push(n);
                    }
                    "votes" => match val {
                        "y" => {
                            votes.push(VoteResult::Won);
                        }
                        "n" => {
                            votes.push(VoteResult::Lost);
                        }
                        "_" => {
                            votes.push(VoteResult::Pending);
                        }
                        _ => {
                            panic!(format!("unknown vote: {}", val));
                        }
                    },
                    _ => {
                        panic!(format!("unknown key: {}", arg.key));
                    }
                }
            }
        }

        // After parsing arguments
        // Collect votes, remain order ids then idsj and remove the duplicate

        let mut voters = ids.clone();
        for id in &idsj {
            if !voters.contains(id) {
                voters.push(*id);
            }
        }

        let c_set: HashSet<u64> = ids.into_iter().collect();
        let cj_set: HashSet<u64> = idsj.into_iter().collect();

        println!("voters: {:?}, idxs: {:?}", voters, idxs);

        // buffer for expected value
        let mut buf = String::new();

        match data.cmd.as_str() {
            "committed" => {
                assert!(groups.is_empty(), "single commit dont need group_ids");

                // checks voters length
                debug_assert_eq!(
                    voters.len(),
                    idxs.len(),
                    "voters number mismatch, {:?}",
                    data.pos
                );

                let mut l: AckIndexer = AckIndexer::default();

                for (i, id) in voters.drain(..).enumerate() {
                    l.insert(
                        id,
                        Index {
                            index: idxs[i],
                            group_id: 0,
                        },
                    );
                }

                if joint {
                    // joint commit (single group)

                    let cc = JointConfig::new_joint(c_set.clone(), cj_set.clone());

                    buf.push_str(&cc.describe(&l));

                    let idx = cc.committed_index(false, &l).0;

                    let a_idx = JointConfig::new_joint(cj_set.clone(), c_set.clone())
                        .committed_index(false, &l)
                        .0;

                    if idx != a_idx {
                        buf.push_str(&format!("{}  <-- via symmetry\n", a_idx));
                    }

                    buf.push_str(&format!("{}\n", idx));
                } else {
                    // majority commit (single group)

                    let c = MajorityConfig::new(c_set.clone());

                    let idx = c.committed_index(false, &l).0;

                    buf.push_str(&c.describe(&l));

                    let a_idx = JointConfig::new_joint(c_set.clone(), HashSet::default())
                        .committed_index(false, &l)
                        .0;

                    if a_idx != idx {
                        buf.push_str(&format!("{}  <-- via zero-joint quorum\n", a_idx));
                    }

                    let a_idx = JointConfig::new_joint(c_set.clone(), c_set.clone())
                        .committed_index(false, &l)
                        .0;

                    if a_idx != idx {
                        buf.push_str(&format!("{}  <-- via self-joint quorum\n", a_idx));
                    }

                    // overlay
                    //
                    // If the committed index was definitely above the currently inspected idx,
                    // the result shouldn't change if we lower it further

                    for (i, &id) in voters.iter().enumerate() {
                        if 0 < idxs[i] && idxs[i] < idx {
                            // Case1: set idx[i] => idx[i] - 1
                            l.insert(
                                id,
                                Index {
                                    index: idxs[i] - 1,
                                    group_id: 0,
                                },
                            );

                            let a_idx = c.clone().committed_index(false, &l).0;
                            if a_idx != idx {
                                buf.push_str(&format!(
                                    "{}  <-- overlaying {}->{}\n",
                                    a_idx,
                                    id,
                                    idxs[i] - 1
                                ));
                            }
                            // Case2: set idx[i] => 0
                            l.insert(
                                id,
                                Index {
                                    index: 0,
                                    group_id: 0,
                                },
                            );

                            let a_idx = c.clone().committed_index(false, &l).0;
                            if a_idx != idx {
                                buf.push_str(&format!("{}  <-- overlaying {}->0\n", a_idx, id));
                            }

                            // Recover
                            l.insert(
                                id,
                                Index {
                                    index: idxs[i],
                                    group_id: 0,
                                },
                            );
                        }
                    }
                    buf.push_str(&format!("{}\n", idx));
                }
            }
            "vote" => {
                // checks voters length
                debug_assert_eq!(
                    voters.len(),
                    votes.len(),
                    "voters number mismatch, {:?}",
                    data.pos
                );

                let mut l: HashMap<u64, bool> = HashMap::default();

                for (i, id) in voters.drain(..).enumerate() {
                    match votes[i] {
                        VoteResult::Won => {
                            l.insert(id, true);
                        }
                        VoteResult::Pending => {}
                        VoteResult::Lost => {
                            l.insert(id, false);
                        }
                    }
                }

                if joint {
                    let r = JointConfig::new_joint(c_set.clone(), cj_set.clone())
                        .vote_result(|id| l.get(&id).cloned());
                    let a_r = JointConfig::new_joint(cj_set.clone(), c_set.clone())
                        .vote_result(|id| l.get(&id).cloned());

                    println!("r: {}", r);
                    println!("r: {:?}", r);

                    if a_r != r {
                        buf.push_str(&format!("{} <-- via symmetry\n", a_r));
                    }
                    buf.push_str(&format!("{}\n", r));
                } else {
                    let c = MajorityConfig::new(c_set.clone());
                    let r = c.vote_result(|id| l.get(&id).cloned());

                    println!("r: {}", r);
                    println!("r: {:?}", r);

                    buf.push_str(&format!("{}\n", r));
                }
            }
            _ => {
                panic!("unknown command: {}", data.cmd);
            }
        }

        buf
    }

    run_test("src/quorum/testdata", test_quorum, &logger)?;
    Ok(())
}
