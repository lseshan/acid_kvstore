package txmanager

import (
	"context"
	"log"

	pbt "github.com/acid_kvstore/proto/package/txmanagerpb"
)

// XXX: If entry not present, send pending

func (ts *TxStore) TxGetRecordState(_ context.Context, in *pbt.TxReq) (*pbt.TxReply, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	resp := new(pbt.TxReply)
	resp.TxId = in.TxContext.TxId
	t, ok := ts.TxRecordStore[in.TxContext.TxId]
	if ok == true {
		resp.Stage = t.TxPhase
	} else {
		resp.Stage = "PENDING"
	}

	log.Printf("TxGetRecordState: TxId %v status: %v", resp.TxId, resp.Stage)

	return resp, nil
}
