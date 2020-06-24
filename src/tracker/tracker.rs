// Config reflects the configuration tracked in a ProgressTracker.
struct Config {
    voters:
}

// ProgressTracker tracks the currently active configuration and the information
// known about the nodes and learners in it. In particular, it tracks the match
// index for each peer which in turn allows reasoning about the committed index.
struct ProgressTracker {

}

type ProgressTracker struct {
    Config

    Progress ProgressMap

    Votes map[uint64]bool

    MaxInflight int
}