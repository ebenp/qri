package core

import (
	"context"
	"testing"

	namesys "gx/ipfs/QmViBzgruNUoLNBnXcx8YWbDNwV8MNGEGKkLo6JGetygdw/go-ipfs/namesys"
)

func TestCheckVersion(t *testing.T) {
	name := prevIPNSName
	ver := lastPubVerHash
	defer func() {
		prevIPNSName = name
		lastPubVerHash = ver
	}()

	res := namesys.NewDNSResolver()

	prevIPNSName = "foo"
	expect := "error resolving name: not a valid domain name"
	if err := CheckVersion(context.Background(), res); err != nil && err.Error() != expect {
		t.Errorf("error mismatch. epxected: '%s', got: '%s'", expect, err.Error())
		return
	}

	// TODO - not workin'
	// if err := CheckVersion(context.Background(), res); err != nil {
	// 	t.Errorf("error checking valid version: %s", err.Error())
	// 	return
	// }

	// lastPubVerHash = "/def/not/good"
	// if err := CheckVersion(context.Background(), res); err != nil && err != ErrUpdateRequired {
	// 	t.Errorf("expected ErrUpdateRequired, got: %s", err.Error())
	// } else if err == nil {
	// 	t.Errorf("expected error, got nil")
	// }
}
