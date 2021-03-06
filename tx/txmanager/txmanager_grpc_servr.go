package txmanager

import (
	"context"
	//"log"
	//"log"
	log "github.com/pingcap-incubator/tinykv/log"

	pbt "github.com/acid_kvstore/proto/package/txmanagerpb"
)

// XXX: If entry not present, send pending

func (ts *TxStore) TxGetRecordState(_ context.Context, in *pbt.TxReq) (*pbt.TxReply, error) {
	resp := new(pbt.TxReply)
	resp.TxId = in.TxContext.TxId
	ts.mu.RLock()
	t, ok := ts.TxRecordStore[in.TxContext.TxId]
	ts.mu.RUnlock()
	if ok == true {
		resp.Stage = t.TxPhase
	} else {
		resp.Stage = "COMMIT"
	}

	log.Infof("TxGetRecordState: TxId %v status: %v", resp.TxId, resp.Stage)

	return resp, nil
}
