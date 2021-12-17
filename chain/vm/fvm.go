package vm

import (
	"context"

	ffi "github.com/filecoin-project/filecoin-ffi"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
)

var _ VMI = (*FVM)(nil)

type FVM struct{}

func NewFVM(ctx context.Context, opts *VMOpts) (*FVM, error) {

	// TODO: Keep the ID and figure out what to do with it :P
	_, err := ffi.CreateFVM(0, opts.Epoch, opts.BaseFee, opts.NetworkVersion)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (vm *FVM) ApplyMessage(ctx context.Context, cmsg types.ChainMsg) (*ApplyRet, error) {
	// TODO
	return nil, nil
}

func (vm *FVM) ApplyImplicitMessage(ctx context.Context, msg *types.Message) (*ApplyRet, error) {
	// TODO
	return nil, nil
}

func (vm *FVM) SetBlockHeight(h abi.ChainEpoch) {
}

func (vm *FVM) Flush(ctx context.Context) (cid.Cid, error) {
	return cid.Undef, nil
}
