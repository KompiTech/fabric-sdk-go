/*
Copyright Kompitech Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package txn enables creating, endorsing and sending transactions to Fabric peers and orderers.
package txn

import (
	reqContext "context"
	"math/rand"
	"sync"

	"github.com/pkg/errors"

	"github.com/hyperledger/fabric-sdk-go/pkg/common/errors/multi"
	"github.com/hyperledger/fabric-sdk-go/pkg/common/providers/fab"
	"github.com/hyperledger/fabric-sdk-go/third_party/github.com/hyperledger/fabric/protos/common"
	pb "github.com/hyperledger/fabric-sdk-go/third_party/github.com/hyperledger/fabric/protos/peer"
	protos_utils "github.com/hyperledger/fabric-sdk-go/third_party/github.com/hyperledger/fabric/protos/utils"
)

// txn section ...

// CreateTxProposal creates and retursn transaction payload
func CreateTxProposal(tx *fab.Transaction) (*common.Payload, error) {
	if tx == nil {
		return nil, errors.New("transaction is nil")
	}
	if tx.Proposal == nil || tx.Proposal.Proposal == nil {
		return nil, errors.New("proposal is nil")
	}
	// the original header
	hdr, err := protos_utils.GetHeader(tx.Proposal.Proposal.Header)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal proposal header failed")
	}
	// serialize the tx
	txBytes, err := protos_utils.GetBytesTransaction(tx.Transaction)
	if err != nil {
		return nil, err
	}

	// create the payload
	payload := common.Payload{Header: hdr, Data: txBytes}

	return &payload, nil
}

// BroadcastSignedPayload will send the given payload to some orderer, picking random endpoints
// until all are exhausted
func BroadcastSignedPayload(reqCtx reqContext.Context, orderers []fab.Orderer, envelope *fab.SignedEnvelope) (*fab.TransactionResponse, error) {
	// Check if orderers are defined
	if len(orderers) == 0 {
		return nil, errors.New("orderers not set")
	}

	return broadcastEnvelope(reqCtx, envelope, orderers)
}

// GetConfigBlock returns a config block response
func GetConfigBlock(reqCtx reqContext.Context, envelope *fab.SignedEnvelope, orderers []fab.Orderer) (*common.Block, error) {
	if len(orderers) == 0 {
		return nil, errors.New("orderers not set")
	}

	// Copy aside the ordering service endpoints
	randOrderers := []fab.Orderer{}
	randOrderers = append(randOrderers, orderers...)

	// Iterate them in a random order and try broadcasting 1 by 1
	var errResp error
	for _, i := range rand.Perm(len(randOrderers)) {
		resp, err := sendEnvelope(reqCtx, envelope, randOrderers[i])
		if err != nil {
			errResp = err
		} else {
			return resp, nil
		}
	}
	return nil, errResp
}

// proposal section ...

// SendProposalCustom sends a TransactionProposal to ProposalProcessor.
func SendProposalCustom(reqCtx reqContext.Context, targets []fab.ProposalProcessor, signedProposal *pb.SignedProposal) ([]*fab.TransactionProposalResponse, error) {

	if signedProposal == nil {
		return nil, errors.New("proposal is required")
	}

	if len(targets) < 1 {
		return nil, errors.New("targets is required")
	}

	for _, p := range targets {
		if p == nil {
			return nil, errors.New("target is nil")
		}
	}

	targets = getTargetsWithoutDuplicates(targets)

	request := fab.ProcessProposalRequest{SignedProposal: signedProposal}

	var responseMtx sync.Mutex
	var transactionProposalResponses []*fab.TransactionProposalResponse
	var wg sync.WaitGroup
	errs := multi.Errors{}

	for _, p := range targets {
		wg.Add(1)
		go func(processor fab.ProposalProcessor) {
			defer wg.Done()

			// TODO: The RPC should be timed-out.
			//resp, err := processor.ProcessTransactionProposal(context.NewRequestOLD(ctx), request)
			resp, err := processor.ProcessTransactionProposal(reqCtx, request)
			if err != nil {
				logger.Debugf("Received error response from txn proposal processing: %s", err)
				responseMtx.Lock()
				errs = append(errs, err)
				responseMtx.Unlock()
				return
			}

			responseMtx.Lock()
			transactionProposalResponses = append(transactionProposalResponses, resp)
			responseMtx.Unlock()
		}(p)
	}
	wg.Wait()

	return transactionProposalResponses, errs.ToError()
}
