package p2p

import (
	"time"
	// "github.com/qri-io/qri/repo"
)

// MtDatasetChanges is a message to announce added / removed datasets to the network
const MtDatasetChanges = MsgType("dataset_changes")

// DatasetChanges describes created & deleted datasets with slices of
// repo DatsetRef strings.
// Because dataset data is immutable, All changes should be describable
// as creations and deletions.
// Dataset names, however, *are* mutable. Renaming is conveyed by listing
// the former ref as deleted & new ref as created.
type DatasetChanges struct {
	Created []string
	Deleted []string
}

// AnnounceDatasetChanges transmits info of dataset changes to
func (n *QriNode) AnnounceDatasetChanges(changes DatasetChanges) error {
	log.Debugf("%s AnnounceDatasetChanges", n.ID)

	msg, err := NewJSONBodyMessage(n.ID, MtDatasetChanges, changes)
	if err != nil {
		return err
	}
	// grab 50 peers & fire off our announcement to them
	return n.SendMessage(msg, nil, n.ClosestConnectedPeers("", 50)...)
}

func (n *QriNode) handleDatasetChanges(ws *WrappedStream, msg Message) (hangup bool) {
	hangup = true

	// only handle messages once, and if they're not too old
	if _, ok := n.msgState.Load(msg.ID); ok || time.Now().After(msg.Deadline) {
		return
	}

	n.msgState.Store(msg.ID, msg.Deadline)

	// n.Repo.Cache()...
	return
}
