package p2p

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/qri-io/qri/repo"
)

func TestAnnounceDatasetChanges(t *testing.T) {
	ctx := context.Background()
	peers, err := NewTestNetwork(ctx, t, 3)
	if err != nil {
		t.Errorf("error creating network: %s", err.Error())
		return
	}

	if err := connectNodes(ctx, peers); err != nil {
		t.Errorf("error connecting peers: %s", err.Error())
	}

	t.Logf("testing AnnounceDatasetChanges message with %d peers", len(peers))
	var wg sync.WaitGroup
	for i, p := range peers {
		wg.Add(1)

		r := make(chan Message)
		p.ReceiveMessages(r)

		go func(p *QriNode) {
			msg := <-r
			t.Log(msg)
			if msg.Type != MtDatasetChanges {
				t.Error("expected only dataset_changes messages")
			}
			wg.Done()
		}(p)

		go func(i int, p *QriNode) {
			if err := p.AnnounceDatasetChanges(DatasetChanges{
				Created: []string{
					repo.DatasetRef{PeerID: p.ID.String(), Name: fmt.Sprintf("dataset-%d", i), Path: fmt.Sprintf("QmFoo%d", i)}.String(),
				},
			}); err != nil {
				t.Errorf("%s error: %s", p.ID.Pretty(), err.Error())
			}
		}(i, p)
	}

	wg.Wait()
}
