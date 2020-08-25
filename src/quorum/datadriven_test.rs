use crate::quorum::{AckIndexer, AckedIndexer, Index};
use crate::{default_logger, HashMap, HashSet, JointConfig, MajorityConfig};
use anyhow::Result;
use datadriven::{run_test, TestData};

#[test]
fn test_data_driven_quorum() -> Result<()> {
    let logger = default_logger();

    fn test_quorum(data: &TestData) -> String {
        // Two majority configs. The first one is always used (though it may
        // be empty) and the second one is used iff joint is true.
        let mut joint = false;

        // The committed indexes for the nodes in the config in the order in
        // which they appear in (ids,idsj), without repetition. An underscore
        // denotes an omission (i.e. no information for this voter); this is
        // different from 0. For example,
        //
        // cfg=(1,2) cfgj=(2,3,4) idxs=(_,5,_,7) initializes the idx for voter 2
        // to 5 and that for voter 4 to 7 (and no others).
        //
        let mut ids: Vec<u64> = Vec::new();
        let mut idsj: Vec<u64> = Vec::new();

        // indexs
        let mut idxs: Vec<Index> = Vec::new();
        // groups_ids
        let mut gps: Vec<u64> = Vec::new();

        // Votes. These are initialized similar to idxs except the only values
        // used are VoteResult.
        let mut votes: Vec<Index> = Vec::new();

        for arg in &data.cmd_args {
            for val in &arg.vals {
                match arg.key.as_str() {
                    "cfg" => {
                        let n: u64 = val.parse().expect("type of n should be u64");
                        ids.push(n);
                    }
                    "cfgj" => {
                        joint = true;

                        if val == "zero" {
                            assert_eq!(arg.vals.len(), 1, "cannot mix 'zero' into configuration")
                        } else {
                            let n: u64 = val.parse().expect("type of n should be u64");
                            idsj.push(n);
                        }
                    }
                    "idx" => {
                        let mut n: u64 = 0;
                        if val != "_" {
                            n = val.parse().expect("type of n should be u64");
                            if n == 0 {
                                panic!("use '_' as 0, check {}", data.pos)
                            }
                        }
                        idxs.push(Index {
                            index: n,
                            group_id: 0,
                        });
                    }
                    "votes" => match val.as_str() {
                        "y" => {
                            votes.push(Index {
                                index: 2,
                                group_id: 0,
                            });
                        }
                        "n" => {
                            votes.push(Index {
                                index: 1,
                                group_id: 0,
                            });
                        }
                        "_" => {
                            votes.push(Index {
                                index: 0,
                                group_id: 0,
                            });
                        }
                        _ => {
                            panic!("unknown vote: {}", val);
                        }
                    },
                    "group" => {
                        let mut n: u64 = 0;
                        if val != "_" {
                            n = val.parse().expect("type of n should be u64");
                            if n == 0 {
                                panic!("use '_' as 0, check {}", data.pos)
                            }
                        }
                        gps.push(n);
                    }
                    _ => {
                        panic!("unknown arg: {}", arg.key);
                    }
                }
            }
        }

        let ids_set: HashSet<u64> = ids.iter().cloned().collect();
        let idsj_set: HashSet<u64> = idsj.iter().cloned().collect();

        // Build the two majority configs.
        let c = MajorityConfig::new(ids_set.clone());
        let cj = MajorityConfig::new(idsj_set.clone());

        let make_lookuper = |idxs: &[Index], ids: &[u64], idsj: &[u64]| -> AckIndexer {
            let mut l = AckIndexer::default();
            // next to consume from idxs
            let mut p: usize = 0;
            let mut ids = ids.to_vec();
            let mut idsj = idsj.to_vec();
            ids.append(&mut idsj);
            for id in ids {
                if !l.contains_key(&id) && p < idxs.len() {
                    l.insert(id, idxs[p]);
                    p += 1;
                }
            }
            l.retain(|_, index| index.index > 0);
            l
        };

        let mut input = idxs.clone();
        if data.cmd == "vote" {
            input = votes.clone();
        }
        let voters: HashSet<u64> = JointConfig::new_joint(ids_set, idsj_set)
            .ids()
            .iter()
            .collect();
        if voters.len() != input.len() {
            format!(
                "error: mismatched input (explicit or _) for voters {:?}: {:?}",
                voters, input
            );
        }

        // buffer for expected value
        let mut buf = String::new();

        match data.cmd.as_str() {
            "committed" | "group_committed" => {
                let mut use_group_commit = false;
                match data.cmd.as_str() {
                    "committed" => {
                        assert!(gps.is_empty(), "single commit don't need group_ids");
                    }
                    "group_committed" => {
                        debug_assert_eq!(
                            idxs.len(),
                            gps.len(),
                            "indexes len is not equal to groups len, check {}",
                            data.pos
                        );

                        for (i, group_id) in gps.iter().enumerate() {
                            idxs[i].group_id = *group_id;
                        }
                        use_group_commit = true;
                    }
                    _ => {}
                }

                let l = make_lookuper(&idxs, &ids, &idsj);

                // Branch based on whether this is a majority or joint quorum
                // test case.
                if !joint {
                    let idx = c.committed_index(use_group_commit, &l);
                    buf.push_str(&c.describe(&l));

                    // Joining a majority with the empty majority should give same result.
                    let a_idx =
                        JointConfig::new_joint_from_configs(c.clone(), MajorityConfig::default())
                            .committed_index(use_group_commit, &l);
                    if a_idx != idx {
                        buf.push_str(&format!("{} <-- via zero-joint quorum\n", a_idx.0));
                    }

                    // Joining a majority with itself should give same result.
                    let a_idx = JointConfig::new_joint_from_configs(c.clone(), c.clone())
                        .committed_index(use_group_commit, &l);
                    if a_idx != idx {
                        buf.push_str(&format!("{} <-- via self-joint quorum\n", a_idx.0));
                    }

                    let overlay = |c: &MajorityConfig,
                                   l: &dyn AckedIndexer,
                                   id: u64,
                                   idx: Index|
                     -> AckIndexer {
                        let mut ll = AckIndexer::default();
                        for iid in c.slice() {
                            if iid == id {
                                ll.insert(iid, idx);
                            } else if let Some(idx) = l.acked_index(iid) {
                                ll.insert(iid, idx);
                            }
                        }
                        ll
                    };

                    for id in c.slice() {
                        if let Some(iidx) = l.acked_index(id) {
                            // iidx.index that equals 0 is deleted in make_lookuper
                            if idx.0 > iidx {
                                let new_idx = Index {
                                    index: iidx.index - 1,
                                    group_id: iidx.group_id,
                                };

                                // If the committed index was definitely above the currently
                                // inspected idx, the result shouldn't change if we lower it
                                // further.
                                let lo = overlay(&c, &l, id, new_idx);
                                let a_idx = c.committed_index(use_group_commit, &lo);
                                if a_idx != idx {
                                    buf.push_str(&format!(
                                        "{} <-- overlaying {}->{}\n",
                                        a_idx.0, id, new_idx
                                    ));
                                }
                                let new_idx = Index {
                                    index: 0,
                                    group_id: iidx.group_id,
                                };
                                let lo = overlay(&c, &l, id, new_idx);
                                let a_idx = c.committed_index(use_group_commit, &lo);
                                if a_idx != idx {
                                    buf.push_str(&format!(
                                        "{} <-- overlaying {}->{}\n",
                                        a_idx.0.index, id, new_idx
                                    ));
                                }
                            }
                        }
                    }
                    buf.push_str(&format!("{}\n", idx.0));
                } else {
                    let cc = JointConfig::new_joint_from_configs(c.clone(), cj.clone());
                    buf.push_str(&cc.describe(&l));
                    let idx = cc.committed_index(use_group_commit, &l);
                    // Interchanging the majorities shouldn't make a difference. If it does, print.
                    let a_idx = JointConfig::new_joint_from_configs(cj, c)
                        .committed_index(use_group_commit, &l);
                    if a_idx != idx {
                        buf.push_str(&format!("{} <-- via symmetry\n", a_idx.0));
                    }
                    buf.push_str(&format!("{}\n", idx.0));
                }
            }
            "vote" => {
                let ll = make_lookuper(&votes, &ids, &idsj);
                let mut l: HashMap<u64, bool> = HashMap::default();

                for (id, v) in ll {
                    l.insert(id, v.index != 1);
                }

                if !joint {
                    // Test a majority quorum.
                    let r = c.vote_result(|id| l.get(&id).cloned());
                    buf.push_str(&format!("{}\n", r));
                } else {
                    // Run a joint quorum test case.
                    let r = JointConfig::new_joint_from_configs(c.clone(), cj.clone())
                        .vote_result(|id| l.get(&id).cloned());

                    // Interchanging the majorities shouldn't make a difference. If it does, print.
                    let ar = JointConfig::new_joint_from_configs(cj, c)
                        .vote_result(|id| l.get(&id).cloned());
                    if ar != r {
                        buf.push_str(&format!("{} <-- via symmetry\n", ar));
                    }
                    buf.push_str(&format!("{}\n", r));
                }
            }
            _ => {
                panic!("unknown command: {}", data.cmd);
            }
        }
        buf
    }

    run_test("src/quorum/testdata", test_quorum, false, &logger)?;
    Ok(())
}
