package repo

import (
	"github.com/qri-io/analytics"
	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/qri/repo/profile"
	"testing"
)

func TestParseDatasetRef(t *testing.T) {
	peernameDatasetRef := DatasetRef{
		Peername: "peername",
	}

	nameDatasetRef := DatasetRef{
		Peername: "peername",
		Name:     "datasetname",
	}

	fullDatasetRef := DatasetRef{
		Peername: "peername",
		Name:     "datasetname",
		Path:     "/network/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y",
	}

	fullIPFSDatasetRef := DatasetRef{
		Peername: "peername",
		Name:     "datasetname",
		Path:     "/ipfs/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y",
	}

	pathOnlyDatasetRef := DatasetRef{
		Path: "/network/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y",
	}

	ipfsOnlyDatasetRef := DatasetRef{
		Path: "/ipfs/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y",
	}

	cases := []struct {
		input  string
		expect DatasetRef
		err    string
	}{
		{"", DatasetRef{}, "cannot parse empty string as dataset reference"},
		{"/peername/", peernameDatasetRef, ""},
		{"/peername", peernameDatasetRef, ""},
		{"/peername/datasetname/", nameDatasetRef, ""},
		{"/peername/datasetname", nameDatasetRef, ""},
		{"/peername/datasetname/@", nameDatasetRef, ""},
		{"/peername/datasetname@", nameDatasetRef, ""},
		{"/peername/datasetname/@/network/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y", fullDatasetRef, ""},
		{"/peername/datasetname/@network/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y", fullDatasetRef, ""},
		{"/peername/datasetname/@/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y", fullIPFSDatasetRef, ""},
		{"/peername/datasetname/@QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y", fullIPFSDatasetRef, ""},
		{"/peername/datasetname@/network/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y", fullDatasetRef, ""},
		{"/peername/datasetname@network/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y", fullDatasetRef, ""},
		{"/peername/datasetname@/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y", fullIPFSDatasetRef, ""},
		{"/peername/datasetname@QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y", fullIPFSDatasetRef, ""},
		{"@/network/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y", pathOnlyDatasetRef, ""},
		{"@network/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y", pathOnlyDatasetRef, ""},
		{"@/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y", ipfsOnlyDatasetRef, ""},
		{"@QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y", ipfsOnlyDatasetRef, ""},
		{"/peername/datasetname/@/network/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y/junk/junk/...", fullDatasetRef, ""},
		{"/peername/datasetname/@network/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y/junk/junk/...", fullDatasetRef, ""},
		{"/peername/datasetname/@/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y/junk/junk/...", fullIPFSDatasetRef, ""},
		{"/peername/datasetname/@QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y/junk/junk/...", fullIPFSDatasetRef, ""},
		{"/peername/datasetname@/network/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y/junk/junk/...", fullDatasetRef, ""},
		{"/peername/datasetname@network/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y/junk/junk/...", fullDatasetRef, ""},
		{"/peername/datasetname@/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y/junk/junk/...", fullIPFSDatasetRef, ""},
		{"/peername/datasetname@QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y/junk/junk/...", fullIPFSDatasetRef, ""},
		{"@/network/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y/junk/junk/...", pathOnlyDatasetRef, ""},
		{"@network/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y/junk/junk/...", pathOnlyDatasetRef, ""},
		{"@/QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y/junk/junk/...", ipfsOnlyDatasetRef, ""},
		{"@QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y/junk/junk/...", ipfsOnlyDatasetRef, ""},
		{"/peername/datasetname@network/bad_hash", DatasetRef{}, "'network/bad_hash' is not a base58 multihash"},
		{"/peername/datasetname@bad_hash/junk/junk..", DatasetRef{}, "'bad_hash/junk/junk..' is not a base58 multihash"},
		{"/peername/datasetname@bad_hash", DatasetRef{}, "'bad_hash' is not a base58 multihash"},
		{"@///*(*)/", DatasetRef{}, "malformed DatasetRef string: @///*(*)/"},
		{"///*(*)/", DatasetRef{}, "malformed DatasetRef string: ///*(*)/"},
		{"@", DatasetRef{}, ""},
		{"///@////", DatasetRef{}, ""},
	}

	for i, c := range cases {
		got, err := ParseDatasetRef(c.input)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if err := CompareDatasetRef(got, c.expect); err != nil {
			t.Errorf("case %d: %s", i, err.Error())
		}
	}
}

func TestMatch(t *testing.T) {
	cases := []struct {
		a, b  string
		match bool
	}{
		{"a/b@/b/QmRdexT18WuAKVX3vPusqmJTWLeNSeJgjmMbaF5QLGHna1", "a/b@/b/QmRdexT18WuAKVX3vPusqmJTWLeNSeJgjmMbaF5QLGHna1", true},
		{"a/b@/b/QmRdexT18WuAKVX3vPusqmJTWLeNSeJgjmMbaF5QLGHna1", "a/b@/b/QmdJgfxj4rocm88PLeEididS7V2cc9nQosA46RpvAnWvDL", true},

		{"a/different_name@/b/QmdJgfxj4rocm88PLeEididS7V2cc9nQosA46RpvAnWvDL", "a/b@/b/QmdJgfxj4rocm88PLeEididS7V2cc9nQosA46RpvAnWvDL", true},
		{"different_peername/b@/b/QmdJgfxj4rocm88PLeEididS7V2cc9nQosA46RpvAnWvDL", "a/b@/b/QmdJgfxj4rocm88PLeEididS7V2cc9nQosA46RpvAnWvDL", true},
	}

	for i, c := range cases {
		a, err := ParseDatasetRef(c.a)
		if err != nil {
			t.Errorf("case %d error parsing dataset ref a: %s", i, err.Error())
			continue
		}
		b, err := ParseDatasetRef(c.b)
		if err != nil {
			t.Errorf("case %d error parsing dataset ref b: %s", i, err.Error())
			continue
		}

		gotA := a.Match(b)
		if gotA != c.match {
			t.Errorf("case %d a.Match", i)
			continue
		}

		gotB := b.Match(a)
		if gotB != c.match {
			t.Errorf("case %d b.Match", i)
			continue
		}
	}
}

func TestEqual(t *testing.T) {
	cases := []struct {
		a, b  string
		equal bool
	}{
		{"a/b@/b/QmRdexT18WuAKVX3vPusqmJTWLeNSeJgjmMbaF5QLGHna1", "a/b@/b/QmRdexT18WuAKVX3vPusqmJTWLeNSeJgjmMbaF5QLGHna1", true},
		{"a/b@/ipfs/QmRdexT18WuAKVX3vPusqmJTWLeNSeJgjmMbaF5QLGHna1", "a/b@/ipfs/QmdJgfxj4rocm88PLeEididS7V2cc9nQosA46RpvAnWvDL", false},

		{"a/different_name@/ipfs/QmRdexT18WuAKVX3vPusqmJTWLeNSeJgjmMbaF5QLGHna1", "a/b@/ipfs/QmdJgfxj4rocm88PLeEididS7V2cc9nQosA46RpvAnWvDL", false},
		{"different_peername/b@/ipfs/QmRdexT18WuAKVX3vPusqmJTWLeNSeJgjmMbaF5QLGHna1", "a/b@/ipfs/QmdJgfxj4rocm88PLeEididS7V2cc9nQosA46RpvAnWvDL", false},
	}

	for i, c := range cases {
		a, err := ParseDatasetRef(c.a)
		if err != nil {
			t.Errorf("case %d error parsing dataset ref a: %s", i, err.Error())
			continue
		}
		b, err := ParseDatasetRef(c.b)
		if err != nil {
			t.Errorf("case %d error parsing dataset ref b: %s", i, err.Error())
			continue
		}

		gotA := a.Equal(b)
		if gotA != c.equal {
			t.Errorf("case %d a.Equal", i)
			continue
		}

		gotB := b.Equal(a)
		if gotB != c.equal {
			t.Errorf("case %d b.Equal", i)
			continue
		}
	}
}

func TestIsEmpty(t *testing.T) {
	cases := []struct {
		ref   DatasetRef
		empty bool
	}{
		{DatasetRef{}, true},
		{DatasetRef{Peername: "a"}, false},
		{DatasetRef{Name: "a"}, false},
		{DatasetRef{Path: "a"}, false},
	}

	for i, c := range cases {
		got := c.ref.IsEmpty()
		if got != c.empty {
			t.Errorf("case %d: %s", i, c.ref)
			continue
		}
	}
}

func TestCompareDatasetRefs(t *testing.T) {
	cases := []struct {
		a, b DatasetRef
		err  string
	}{
		{DatasetRef{}, DatasetRef{}, ""},
		{DatasetRef{Name: "a"}, DatasetRef{}, "name mismatch. a != "},
		{DatasetRef{Peername: "a"}, DatasetRef{}, "peername mismatch. a != "},
		{DatasetRef{Path: "a"}, DatasetRef{}, "path mismatch. a != "},
	}

	for i, c := range cases {
		err := CompareDatasetRef(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mistmatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}
}

func TestCanonicalize(t *testing.T) {
	repo, err := NewMemRepo(&profile.Profile{Peername: "lucille"}, memfs.NewMapstore(), MemPeers{}, &analytics.Memstore{})
	if err != nil {
		t.Errorf("error allocating mem repo: %s", err.Error())
		return
	}

	cases := []struct {
		input  string
		expect string
		err    string
	}{
		{"me/foo", "lucille/foo", ""},
		{"you/foo", "you/foo", ""},
		{"me/ball@/ipfs/QmRdexT18WuAKVX3vPusqmJTWLeNSeJgjmMbaF5QLGHna1", "lucille/ball@ipfs/QmRdexT18WuAKVX3vPusqmJTWLeNSeJgjmMbaF5QLGHna1", ""},
		// TODO - add tests that show path fulfillment
	}

	for i, c := range cases {
		ref, err := ParseDatasetRef(c.input)
		if err != nil {
			t.Errorf("case %d unexpected dataset ref parse error: %s", i, err.Error())
			continue
		}
		got := &ref

		err = CanonicalizeDatasetRef(repo, got)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if got.String() != c.expect {
			t.Errorf("case %d expected: %s, got: %s", i, c.expect, got)
			continue
		}
	}
}