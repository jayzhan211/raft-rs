use super::errors::Result;
use std::collections::HashMap;

mod joint;
mod majority;

type Index = u64;
type VoteResult = u8;
type MapAckIndexer = HashMap<u64, Index>;

const VOTE_PENDING: VoteResult = 1;
const VOTE_LOST: VoteResult = 2;
const VOTE_WON: VoteResult = 3;

pub trait AckedIndexr {
    fn acked_index(&self, voter_id: u64) -> (Index, bool);
}

impl AckedIndex for MapAckIndexer {
    fn acked_index(&self, id: u64) -> Result<Index> {
        self.get(&id).unwrap().into()
    }
}
