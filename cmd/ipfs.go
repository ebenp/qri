package cmd

import (
	ipfs "github.com/qri-io/cafs/ipfs"
	"github.com/spf13/viper"
)

func GetIpfsFilestore() (*ipfs.Filestore, error) {
	return ipfs.NewFilestore(func(cfg *ipfs.StoreCfg) {
		cfg.FsRepoPath = viper.GetString(IpfsFsPath)
		cfg.Online = false
	})
}
