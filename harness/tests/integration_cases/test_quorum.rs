use raft::util::majority;
use raft::{AckIndexer, HashMap, HashSet, Index, JointConfig, MajorityConfig, VoteResult};

#[test]
fn test_joint_commit_single_group() {
    let mut test_cases = vec![
        // [1] No difference between a simple majority quorum
        // and a simple majority quorum joint with an empty majority quorum.
        (vec![1, 2, 3], vec![0; 0], vec![100, 101, 99], 100),
        // [2] Joint nonoverlapping singleton quorums.
        (vec![1], vec![2], vec![0, 0], 0),
        // [3] Voter 1 has 100 committed, 2 nothing. This means we definitely won't commit
        // past 100.
        (vec![1], vec![2], vec![100, 0], 0),
        // [4] Committed index collapses once both majorities do, to the lower index.
        (vec![1], vec![2], vec![13, 100], 13),
        // [5] Joint overlapping (i.e. identical) singleton quorum.
        (vec![1], vec![1], vec![0], 0),
        (vec![1], vec![1], vec![100], 100),
        // [6] Two-node config joint with non-overlapping single node config
        (vec![1, 3], vec![2], vec![0, 0, 0], 0),
        (vec![1, 3], vec![2], vec![100, 0, 0], 0),
        // [7] 1 has 100 committed, 2 has 50 (collapsing half of the joint quorum to 50).
        (vec![1, 3], vec![2], vec![100, 0, 50], 0),
        // [8] 2 reports 45, collapsing the other half (to 45).
        (vec![1, 3], vec![2], vec![100, 45, 50], 45),
        //
        // test_cases: 11
        // [9] Two-node config with overlapping single-node config.
        (vec![1, 2], vec![2], vec![0, 0], 0),
        // [10] 1 reports 100.
        (vec![1, 2], vec![2], vec![100, 0], 0),
        // [11] 2 reports 100.
        (vec![1, 2], vec![2], vec![0, 100], 0),
        (vec![1, 2], vec![2], vec![50, 100], 50),
        (vec![1, 2], vec![2], vec![100, 50], 50),
        // [12] Joint non-overlapping two-node configs.
        (vec![1, 2], vec![3, 4], vec![50, 0, 0, 0], 0),
        (vec![1, 2], vec![3, 4], vec![50, 0, 49, 0], 0),
        (vec![1, 2], vec![3, 4], vec![50, 48, 49, 0], 0),
        (vec![1, 2], vec![3, 4], vec![50, 48, 49, 47], 47),
        // [13] Joint overlapping two-node configs.
        (vec![1, 2], vec![2, 3], vec![0, 0, 0], 0),
        //
        // test_cases: 21
        (vec![1, 2], vec![2, 3], vec![100, 0, 0], 0),
        (vec![1, 2], vec![2, 3], vec![0, 100, 0], 0),
        (vec![1, 2], vec![2, 3], vec![0, 100, 99], 0),
        (vec![1, 2], vec![2, 3], vec![101, 100, 99], 99),
        // [14] Joint identical two-node configs.
        (vec![1, 2], vec![1, 2], vec![0, 0], 0),
        (vec![1, 2], vec![1, 2], vec![0, 40], 0),
        (vec![1, 2], vec![1, 2], vec![41, 40], 40),
        // [15] Joint disjoint three-node configs.
        (vec![1, 2, 3], vec![4, 5, 6], vec![0; 6], 0),
        (vec![1, 2, 3], vec![4, 5, 6], vec![100, 0, 0, 0, 0, 0], 0),
        (vec![1, 2, 3], vec![4, 5, 6], vec![100, 0, 0, 900, 0, 0], 0),
        //
        // test_cases: 31
        (vec![1, 2, 3], vec![4, 5, 6], vec![100, 99, 0, 0, 0, 0], 0),
        (vec![1, 2, 3], vec![4, 5, 6], vec![100, 99, 98, 0, 0, 0], 0),
        // [16] First quorum <= 99, second one <= 97. Both quorums guarantee that 90 is committed
        (vec![1, 2, 3], vec![4, 5, 6], vec![0, 99, 90, 97, 95, 0], 90),
        // [17] First quorum collapsed to 92. Second one already had at least 95 committed,
        // so the result also collapses.
        (
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![92, 99, 90, 97, 95, 0],
            92,
        ),
        // [18] Second quorum collapses, but nothing changes in the output.
        (
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![92, 99, 90, 97, 95, 77],
            92,
        ),
        // [19] Joint overlapping three-node configs.
        (vec![1, 2, 3], vec![1, 4, 5], vec![0; 5], 0),
        (vec![1, 2, 3], vec![1, 4, 5], vec![100, 0, 0, 0, 0], 0),
        (vec![1, 2, 3], vec![1, 4, 5], vec![100, 100, 0, 0, 0], 0),
        (vec![1, 2, 3], vec![1, 4, 5], vec![100, 101, 100, 0, 0], 0),
        // [20] Second quorum could commit either 98 or 99, but first quorum is open.
        (vec![1, 2, 3], vec![1, 4, 5], vec![0, 100, 0, 99, 98], 0),
        //
        // test_cases: 41
        // [21] Additionally, first quorum can commit either 100 or 99
        (vec![1, 2, 3], vec![1, 4, 5], vec![0, 100, 99, 99, 98], 98),
        (vec![1, 2, 3], vec![1, 4, 5], vec![1, 100, 99, 99, 98], 98),
        (vec![1, 2, 3], vec![1, 4, 5], vec![100, 100, 99, 99, 98], 99),
        // [22] More overlap.
        (vec![1, 2, 3], vec![2, 3, 4], vec![0; 4], 0),
        (vec![1, 2, 3], vec![2, 3, 4], vec![0, 100, 99, 0], 99),
        (vec![1, 2, 3], vec![2, 3, 4], vec![100, 100, 99, 0], 99),
        (vec![1, 2, 3], vec![2, 3, 4], vec![100, 0, 0, 101], 0),
        (vec![1, 2, 3], vec![2, 3, 4], vec![100, 99, 0, 101], 99),
        // [23] Identical
        (vec![1, 2, 3], vec![1, 2, 3], vec![50, 45, 0], 45),
    ];

    for (i, (cfg, cfgj, idx, expected_index)) in test_cases.drain(..).enumerate() {
        let cfg_set: HashSet<_> = cfg.iter().cloned().collect::<HashSet<_>>();
        let cfgj_set: HashSet<_> = cfgj.iter().cloned().collect::<HashSet<_>>();

        let c = MajorityConfig::new(cfg_set);
        let cj = MajorityConfig::new(cfgj_set);

        let mut voters = vec![];
        voters.extend_from_slice(&cfg);
        voters.extend_from_slice(&cfgj);
        let s: std::collections::HashSet<_> = voters.drain(..).collect();
        voters.extend(s.into_iter());
        voters.sort();

        assert_eq!(
            voters.len(),
            idx.len(),
            "[test_cases #{}] error: mismatched input for voters {:?}: {:?}, check out test_cases",
            i + 1,
            voters,
            idx
        );

        let mut l: AckIndexer = AckIndexer::default();

        for (i, id) in voters.drain(..).enumerate() {
            l.insert(
                id,
                Index {
                    index: idx[i],
                    group_id: 0,
                },
            );
        }

        let (index1, _) = JointConfig::new_joint(c.clone(), cj.clone()).committed_index(false, &l);
        let (index2, _) = JointConfig::new_joint(cj, c).committed_index(false, &l);

        assert_eq!(index1, index2, "[test_cases #{}] Interchanging the majorities shouldn't make a difference. If it does, print.", i+1);

        assert_eq!(
            index1,
            expected_index,
            "[test_cases #{}] index does not match expected value",
            i + 1
        )
    }
}

#[test]
fn test_joint_vote() {
    let mut test_cases = vec![
        // votes 0 => vote missing, 1 => vote no, 2 => vote yes

        // [1] Empty joint config wins all votes. This isn't used in production.
        (vec![], vec![], vec![], VoteResult::Won),
        // [2] trivial configs
        (vec![1], vec![], vec![0], VoteResult::Pending),
        (vec![1], vec![], vec![2], VoteResult::Won),
        (vec![1], vec![], vec![1], VoteResult::Lost),
        (vec![1], vec![1], vec![0], VoteResult::Pending),
        (vec![1], vec![1], vec![1], VoteResult::Lost),
        (vec![1], vec![1], vec![2], VoteResult::Won),
        (vec![1], vec![2], vec![0, 0], VoteResult::Pending),
        (vec![1], vec![2], vec![1, 0], VoteResult::Lost),
        (vec![1], vec![2], vec![2, 0], VoteResult::Pending),
        //
        // test_cases: 11
        (vec![1], vec![2], vec![1, 1], VoteResult::Lost),
        (vec![1], vec![2], vec![2, 2], VoteResult::Won),
        (vec![1], vec![2], vec![1, 2], VoteResult::Lost),
        (vec![1], vec![2], vec![2, 1], VoteResult::Lost),
        // [3] two nodes configs
        (vec![1, 2], vec![3, 4], vec![0; 4], VoteResult::Pending),
        (
            vec![1, 2],
            vec![3, 4],
            vec![2, 0, 0, 0],
            VoteResult::Pending,
        ),
        (
            vec![1, 2],
            vec![3, 4],
            vec![2, 2, 0, 0],
            VoteResult::Pending,
        ),
        (vec![1, 2], vec![3, 4], vec![2, 2, 1, 0], VoteResult::Lost),
        (vec![1, 2], vec![3, 4], vec![2, 2, 1, 1], VoteResult::Lost),
        (vec![1, 2], vec![3, 4], vec![2, 2, 2, 1], VoteResult::Lost),
        //
        // test_cases: 21
        (vec![1, 2], vec![3, 4], vec![2, 2, 2, 2], VoteResult::Won),
        (vec![1, 2], vec![2, 3], vec![0, 0, 0], VoteResult::Pending),
        (vec![1, 2], vec![2, 3], vec![0, 1, 0], VoteResult::Lost),
        (vec![1, 2], vec![2, 3], vec![2, 2, 0], VoteResult::Pending),
        (vec![1, 2], vec![2, 3], vec![2, 2, 1], VoteResult::Lost),
        (vec![1, 2], vec![2, 3], vec![2, 2, 2], VoteResult::Won),
        (vec![1, 2], vec![1, 2], vec![0, 0], VoteResult::Pending),
        (vec![1, 2], vec![1, 2], vec![2, 0], VoteResult::Pending),
        (vec![1, 2], vec![1, 2], vec![2, 1], VoteResult::Lost),
        (vec![1, 2], vec![1, 2], vec![1, 0], VoteResult::Lost),
        //
        //  test_cases: 31
        (vec![1, 2], vec![1, 2], vec![1, 1], VoteResult::Lost),
        // [4] Simple example for overlapping three node configs.
        (
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![0; 4],
            VoteResult::Pending,
        ),
        (
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![0, 1, 0, 0],
            VoteResult::Pending,
        ),
        (
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![0, 1, 1, 0],
            VoteResult::Lost,
        ),
        (
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![0, 2, 2, 0],
            VoteResult::Won,
        ),
        (
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![2, 2, 0, 0],
            VoteResult::Pending,
        ),
        (
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![2, 2, 1, 0],
            VoteResult::Pending,
        ),
        (
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![2, 2, 1, 1],
            VoteResult::Lost,
        ),
        (
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![2, 2, 1, 2],
            VoteResult::Won,
        ),
    ];
    for (i, (cfg, cfgj, votes, expected_vote_result)) in test_cases.drain(..).enumerate() {
        let cfg_set: HashSet<_> = cfg.iter().cloned().collect::<HashSet<_>>();
        let cfgj_set: HashSet<_> = cfgj.iter().cloned().collect::<HashSet<_>>();

        let c = MajorityConfig::new(cfg_set);
        let cj = MajorityConfig::new(cfgj_set);

        let mut voters = vec![];
        voters.extend_from_slice(&cfg);
        voters.extend_from_slice(&cfgj);
        let s: HashSet<_> = voters.drain(..).collect();
        voters.extend(s.into_iter());
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

        let vote_result1 =
            JointConfig::new_joint(c.clone(), cj.clone()).vote_result(|id| l.get(&id).cloned());
        let vote_result2 = JointConfig::new_joint(cj, c).vote_result(|id| l.get(&id).cloned());

        assert_eq!(vote_result1,vote_result2, "[test_cases #{}] Interchanging the majorities shouldn't make a difference. If it does, print.", i+1);
        assert_eq!(
            vote_result1,
            expected_vote_result,
            "[test_cases #{}] vote_result does not match expected value",
            i + 1
        )
    }
}

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
fn test_joint_commit_multi_group() {
    let mut test_cases = vec![
        (vec![], vec![], vec![], vec![], u64::MAX, true),
        (
            vec![1, 2, 3],
            vec![],
            vec![77, 88, 99],
            vec![1, 1, 1],
            88,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![],
            vec![77, 88, 99],
            vec![1, 1, 2],
            88,
            true,
        ),
        (
            vec![1, 2, 3],
            vec![],
            vec![77, 88, 99],
            vec![2, 1, 1],
            77,
            true,
        ),
        (
            vec![1, 2, 3],
            vec![],
            vec![77, 88, 99],
            vec![0, 1, 1],
            77,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![],
            vec![77, 88, 99],
            vec![0, 1, 2],
            88,
            true,
        ),
        (
            vec![1, 2, 3],
            vec![],
            vec![77, 88, 99],
            vec![1, 2, 0],
            77,
            true,
        ),
        (vec![1], vec![2], vec![7, 8], vec![1, 2], 7, false),
        (vec![1], vec![2], vec![7, 8], vec![1, 1], 7, false),
        (vec![1], vec![2], vec![7, 8], vec![1, 0], 7, false),
        //
        // test_cases: 11
        (vec![1, 2], vec![3], vec![7, 8, 9], vec![1, 1, 1], 7, false),
        (vec![1, 2], vec![3], vec![7, 8, 9], vec![1, 1, 2], 7, false),
        (vec![1, 2], vec![3], vec![7, 8, 9], vec![1, 2, 1], 7, false),
        (
            vec![1, 2],
            vec![3],
            vec![77, 88, 9],
            vec![1, 1, 1],
            9,
            false,
        ),
        (
            vec![1, 2],
            vec![3],
            vec![77, 88, 9],
            vec![0, 1, 1],
            9,
            false,
        ),
        (
            vec![1, 2],
            vec![3],
            vec![77, 88, 79],
            vec![0, 1, 1],
            77,
            false,
        ),
        (vec![1, 2], vec![2], vec![4, 5], vec![1, 1], 4, false),
        (vec![1, 2], vec![2], vec![4, 5], vec![1, 2], 4, false),
        (vec![1, 2], vec![2], vec![44, 5], vec![1, 2], 5, false),
        (vec![1, 2], vec![2], vec![4, 5], vec![1, 0], 4, false),
        //
        // test_cases: 21
        (
            vec![1, 2],
            vec![3, 4],
            vec![1, 2, 3, 4],
            vec![1, 1, 1, 1],
            1,
            false,
        ),
        (
            vec![1, 2],
            vec![3, 4],
            vec![1, 2, 3, 4],
            vec![1, 1, 1, 2],
            1,
            false,
        ),
        (
            vec![1, 2],
            vec![3, 4],
            vec![3, 4, 3, 4],
            vec![1, 1, 1, 2],
            3,
            false,
        ),
        (
            vec![1, 2],
            vec![3, 4],
            vec![3, 4, 3, 4],
            vec![2, 1, 1, 2],
            3,
            true,
        ),
        (
            vec![1, 2],
            vec![3, 4],
            vec![3, 4, 33, 44],
            vec![2, 1, 1, 2],
            3,
            true,
        ),
        (
            vec![1, 2],
            vec![3, 4],
            vec![3, 4, 33, 44],
            vec![0, 1, 1, 2],
            3,
            false,
        ),
        (
            vec![1, 2],
            vec![3, 4],
            vec![3, 4, 33, 44],
            vec![1, 1, 2, 2],
            3,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![3, 4, 5],
            vec![3, 4, 5, 6, 7],
            vec![1, 1, 1, 1, 1],
            4,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![3, 4, 5],
            vec![3, 44, 55, 6, 7],
            vec![1, 1, 1, 1, 1],
            7,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![3, 4, 5],
            vec![3, 44, 55, 6, 7],
            vec![1, 1, 1, 1, 2],
            7,
            false,
        ),
        //
        // test_cases: 31
        (
            vec![1, 2, 3],
            vec![3, 4, 5],
            vec![3, 44, 55, 6, 7],
            vec![1, 1, 2, 1, 2],
            6,
            true,
        ),
        (
            vec![1, 2, 3],
            vec![3, 4, 5],
            vec![3, 44, 55, 6, 7],
            vec![0, 1, 2, 1, 2],
            6,
            true,
        ),
        (
            vec![1, 2, 3],
            vec![3, 4, 5],
            vec![3, 44, 55, 6, 7],
            vec![1, 0, 2, 1, 2],
            3,
            true,
        ),
        (
            vec![1, 2, 3],
            vec![3, 4, 5],
            vec![3, 44, 55, 6, 7],
            vec![1, 2, 0, 1, 2],
            3,
            true,
        ),
        (
            vec![1, 2, 3],
            vec![3, 4, 5],
            vec![3, 44, 5, 66, 7],
            vec![1, 2, 0, 1, 2],
            3,
            true,
        ),
        (
            vec![1, 2, 3],
            vec![3, 4, 5],
            vec![3, 44, 5, 66, 7],
            vec![1, 1, 0, 2, 2],
            3,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5, 6],
            vec![1, 1, 1, 1],
            4,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5, 6],
            vec![1, 2, 1, 1],
            4,
            true,
        ),
        (
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5, 6],
            vec![1, 2, 2, 1],
            3,
            true,
        ),
        (
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5, 6],
            vec![1, 2, 2, 2],
            3,
            false,
        ),
        //
        // test_cases: 41
        (
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5, 6],
            vec![2, 1, 0, 2],
            3,
            true,
        ),
        (
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5, 6],
            vec![1, 1, 0, 2],
            3,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![1, 2, 3, 4, 5, 6],
            vec![1, 1, 1, 1, 1, 1],
            2,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![11, 22, 33, 4, 5, 6],
            vec![1, 1, 1, 1, 1, 1],
            5,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![11, 2, 33, 4, 55, 6],
            vec![1, 1, 1, 1, 1, 1],
            6,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![1, 2, 3, 4, 5, 6],
            vec![1, 1, 1, 2, 2, 2],
            2,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![1, 2, 3, 4, 5, 6],
            vec![1, 1, 1, 1, 1, 2],
            2,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![1, 2, 3, 4, 5, 6],
            vec![1, 1, 2, 1, 1, 2],
            2,
            true,
        ),
        (
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![1, 2, 3, 4, 5, 6],
            vec![1, 1, 2, 1, 1, 0],
            2,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![1, 2, 3, 4, 5, 6],
            vec![1, 1, 0, 1, 1, 2],
            1,
            false,
        ),
        //
        // test_cases: 51
        (
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![1, 2, 3, 4, 5, 6],
            vec![0, 1, 2, 0, 1, 2],
            2,
            true,
        ),
        (
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![1, 2, 3, 4, 5, 6],
            vec![0, 1, 2, 0, 1, 1],
            2,
            false,
        ),
        (
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![1, 2, 3, 4, 5, 6],
            vec![0, 1, 1, 0, 1, 2],
            1,
            false,
        ),
    ];

    for (i, (cfg, cfgj, idx, group_ids, expected_index, expected_use_group_commit)) in
        test_cases.drain(..).enumerate()
    {
        let cfg_set: HashSet<_> = cfg.iter().cloned().collect::<HashSet<_>>();
        let cfgj_set: HashSet<_> = cfgj.iter().cloned().collect::<HashSet<_>>();

        let c = MajorityConfig::new(cfg_set);
        let cj = MajorityConfig::new(cfgj_set);

        let mut voters = vec![];
        voters.extend_from_slice(&cfg);
        voters.extend_from_slice(&cfgj);
        let s: std::collections::HashSet<_> = voters.drain(..).collect();
        voters.extend(s.into_iter());
        voters.sort();

        assert_eq!(
            voters.len(),
            idx.len(),
            "[test_cases #{}] error: mismatched input for voters {:?}: {:?}, check out test_cases",
            i + 1,
            voters,
            idx
        );

        let mut l: AckIndexer = AckIndexer::default();

        for (i, id) in voters.drain(..).enumerate() {
            l.insert(
                id,
                Index {
                    index: idx[i],
                    group_id: group_ids[i],
                },
            );
        }

        let (index1, use_group_commit1) =
            JointConfig::new_joint(c.clone(), cj.clone()).committed_index(true, &l);
        let (index2, use_group_commit2) = JointConfig::new_joint(cj, c).committed_index(true, &l);

        assert_eq!(index1, index2, "[test_cases #{}] Interchanging the majorities shouldn't make a difference. If it does, print.", i+1);
        assert_eq!(use_group_commit1, use_group_commit2, "[test_cases #{}] Interchanging the majorities shouldn't make a difference. If it does, print.", i+1);

        assert_eq!(
            index1,
            expected_index,
            "[test_cases #{}] index does not match expected value",
            i + 1
        );
        assert_eq!(
            use_group_commit1,
            expected_use_group_commit,
            "[test_cases #{}] index is not computed by group commit",
            i + 1
        );
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
