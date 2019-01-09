/*
Copyright Kompitech Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package resource provides access to fabric network resource management, typically using system channel queries.
package resource

import (
	reqContext "context"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	ab "github.com/hyperledger/fabric-sdk-go/internal/github.com/hyperledger/fabric/protos/orderer"
	"github.com/hyperledger/fabric-sdk-go/pkg/common/errors/multi"
	"github.com/hyperledger/fabric-sdk-go/pkg/common/errors/retry"
	"github.com/hyperledger/fabric-sdk-go/pkg/common/providers/fab"
	contextImpl "github.com/hyperledger/fabric-sdk-go/pkg/context"
	ccomm "github.com/hyperledger/fabric-sdk-go/pkg/core/config/comm"
	"github.com/hyperledger/fabric-sdk-go/pkg/fab/txn"
	"github.com/hyperledger/fabric-sdk-go/third_party/github.com/hyperledger/fabric/protos/common"
	pb "github.com/hyperledger/fabric-sdk-go/third_party/github.com/hyperledger/fabric/protos/peer"
)

// resource section ...

// GetSaveChannelPayload returns payload for create or update channel call
func GetSaveChannelPayload(reqCtx reqContext.Context, request CreateChannelRequest, opts ...Opt) (*common.Payload, error) {
	if request.Orderer == nil {
		return nil, errors.New("missing orderer request parameter for the initialize channel")
	}

	if request.Name == "" {
		return nil, errors.New("missing name request parameter for the new channel")
	}

	if request.Config == nil {
		return nil, errors.New("missing envelope request parameter containing the configuration of the new channel")
	}

	if request.Signatures == nil {
		return nil, errors.New("missing signatures request parameter for the new channel")
	}

	ctx, ok := contextImpl.RequestClientContext(reqCtx)
	if !ok {
		return nil, errors.New("creation of transaction header failed, failed to extract client context from reqContext")
	}
	txh, err := txn.NewHeader(ctx, request.Name)
	if err != nil {
		return nil, errors.WithMessage(err, "creation of transaction header failed")
	}

	return getSaveChannelPayload(reqCtx, txh, request)
}

// GetBlockPayload returns config block payload
func GetBlockPayload(reqCtx reqContext.Context, channelName string, position string, index uint64) (*common.Payload, error) {
	if position == "newest" {
		return retrieveBlockPayload(reqCtx, channelName, newNewestSeekPosition())
	} else if position == "specific" {
		return retrieveBlockPayload(reqCtx, channelName, newSpecificSeekPosition(index))
	}
	return nil, errors.New("failed to get block payload")
}

// SendJoinChannelProposal sends a join channel proposal to the target peer.
func SendJoinChannelProposal(reqCtx reqContext.Context, targets []fab.ProposalProcessor, signedProposal *pb.SignedProposal, opts ...Opt) error {

	var errors1 multi.Errors
	var mutex sync.Mutex
	var wg sync.WaitGroup

	wg.Add(len(targets))

	for _, t := range targets {
		target := t
		go func() {
			defer wg.Done()
			if _, _, err := QueryChannel(reqCtx, []fab.ProposalProcessor{target}, signedProposal, opts...); err != nil {
				mutex.Lock()
				errors1 = append(errors1, err)
				mutex.Unlock()
			}
		}()
	}

	wg.Wait()

	return errors1.ToError()
}

// GetJoinChannelTxProposal returns tx proposal to list joined channels on target.
func GetJoinChannelTxProposal(reqCtx reqContext.Context, request JoinChannelRequest) (*fab.TransactionProposal, error) {

	if request.GenesisBlock == nil {
		return nil, errors.New("missing block input parameter with the required genesis block")
	}

	cir, err := createJoinChannelInvokeRequest(request.GenesisBlock)
	if err != nil {
		return nil, errors.WithMessage(err, "creation of join channel invoke request failed")
	}

	tp, err := getTxProposal(reqCtx, cir, fab.SystemChannel)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get join channel proposal")
	}

	return tp, nil
}

func getSaveChannelPayload(reqCtx reqContext.Context, txh *txn.TransactionHeader, request CreateChannelRequest) (*common.Payload, error) {

	configUpdateEnvelope := &common.ConfigUpdateEnvelope{
		ConfigUpdate: request.Config,
		Signatures:   request.Signatures,
	}
	configUpdateEnvelopeBytes, err := proto.Marshal(configUpdateEnvelope)
	if err != nil {
		return nil, errors.Wrap(err, "marshal configUpdateEnvelope failed")
	}

	ctx, ok := contextImpl.RequestClientContext(reqCtx)
	if !ok {
		return nil, errors.New("failed get client context from reqContext for Creating ChannelHeader")
	}

	hash, err := ccomm.TLSCertHash(ctx.EndpointConfig())
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get tls cert hash")
	}

	channelHeaderOpts := txn.ChannelHeaderOpts{
		TxnHeader:   txh,
		TLSCertHash: hash,
	}
	channelHeader, err := txn.CreateChannelHeader(common.HeaderType_CONFIG_UPDATE, channelHeaderOpts)
	if err != nil {
		return nil, errors.WithMessage(err, "CreateChannelHeader failed")
	}

	payload, err := txn.CreatePayload(txh, channelHeader, configUpdateEnvelopeBytes)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create save channel payload")
	}
	return payload, nil
}

// BroadcastSignedPayload broadcasts signed envelope to orderer
func BroadcastSignedPayload(reqCtx reqContext.Context, orderer fab.Orderer, envelope *fab.SignedEnvelope) (*fab.TransactionResponse, error) {

	resp, err := txn.BroadcastSignedPayload(reqCtx, []fab.Orderer{orderer}, envelope)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to broadcast signed payload")
	}
	return resp, nil
}

// GetQueryChannelsPayload returns tx proposal to list channels that a peer has joined.
func GetQueryChannelsPayload(reqCtx reqContext.Context) (*fab.TransactionProposal, error) {
	cir := createChannelsInvokeRequest()
	payload, err := getTxProposal(reqCtx, cir, fab.SystemChannel)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get joined channels payload")
	}
	return payload, nil
}

// GetJoinedChannels queries the names of all the channels that a peer has joined.
func GetJoinedChannels(reqCtx reqContext.Context, peer fab.ProposalProcessor, signedProposal *pb.SignedProposal, opts ...Opt) (*pb.ChannelQueryResponse, error) {

	if peer == nil {
		return nil, errors.New("peer required")
	}
	targets := []fab.ProposalProcessor{peer}

	payload, _, err := QueryChannel(reqCtx, targets, signedProposal, opts...)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get joined channels")
	}

	response := new(pb.ChannelQueryResponse)
	err = proto.Unmarshal(payload, response)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal ChannelQueryResponse failed")
	}
	return response, nil
}

// GetExistingCC queries the chaincodes on a peer.
// Returns the details of all chaincodes installed or instantiated on a peer.
func GetExistingCC(reqCtx reqContext.Context, peer fab.ProposalProcessor, signedProposal *pb.SignedProposal, opts ...Opt) (*pb.ChaincodeQueryResponse, error) {

	if peer == nil {
		return nil, errors.New("peer required")
	}
	targets := []fab.ProposalProcessor{peer}

	payload, _, err := QueryChannel(reqCtx, targets, signedProposal, opts...)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get installed or instantiated channels")
	}

	response := new(pb.ChaincodeQueryResponse)
	err = proto.Unmarshal(payload, response)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal ChaincodeQueryResponse failed")
	}

	return response, nil
}

// QueryInstalledChaincodes returns tx proposal to list installed chaincodes on a peer
func GetExistingCCProposal(reqCtx reqContext.Context, queryType, channelID string) (*fab.TransactionProposal, error) {

	cir := fab.ChaincodeInvokeRequest{}
	switch strings.ToLower(queryType) {
	case "installed":
		cir = createInstalledChaincodesInvokeRequest()
	case "instantiated":
		cir = createChaincodeInvokeRequest()
	default:
		return nil, errors.New("Error: queryType unknown. Must be either installed or instantiated")
	}

	prop, err := getTxProposal(reqCtx, cir, channelID)
	if err != nil {
		return nil, errors.WithMessage(err, "get installed or instantiated proposal failed")
	}

	return prop, nil
}

// GetCCInstallProposal return tx propsal to install CC on one or more peers.
func GetCCInstallProposal(reqCtx reqContext.Context, req InstallChaincodeRequest) (*fab.TransactionProposal, error) {

	if req.Name == "" {
		return nil, errors.New("chaincode name required")
	}
	if req.Path == "" {
		return nil, errors.New("chaincode path required")
	}
	if req.Version == "" {
		return nil, errors.New("chaincode version required")
	}
	if req.Package == nil {
		return nil, errors.New("chaincode package is required")
	}

	propReq := ChaincodeInstallRequest{
		Name:    req.Name,
		Path:    req.Path,
		Version: req.Version,
		Package: &ChaincodePackage{
			Type: req.Package.Type,
			Code: req.Package.Code,
		},
	}

	ctx, ok := contextImpl.RequestClientContext(reqCtx)
	if !ok {
		return nil, errors.New("failed get client context from reqContext for txn header")
	}

	txh, err := txn.NewHeader(ctx, fab.SystemChannel)
	if err != nil {
		return nil, errors.WithMessage(err, "create transaction ID failed")
	}

	prop, err := CreateChaincodeInstallProposal(txh, propReq)
	if err != nil {
		return nil, errors.WithMessage(err, "creation of install chaincode proposal failed")
	}
	return prop, nil
}

// QueryChannel send signed proposal to targets
func QueryChannel(reqCtx reqContext.Context, targets []fab.ProposalProcessor, signedProposal *pb.SignedProposal, opts ...Opt) ([]byte, []*fab.TransactionProposalResponse, error) {

	optionsValue := getOpts(opts...)

	resp, err := retry.NewInvoker(retry.New(optionsValue.retry)).Invoke(
		func() (interface{}, error) {
			return txn.SendProposalCustom(reqCtx, targets, signedProposal)
		},
	)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "failed to query channel")
	}

	tpr := resp.([]*fab.TransactionProposalResponse)
	err = validateResponse(tpr[0])
	if err != nil {
		return nil, nil, errors.WithMessage(err, "transaction proposal failed")
	}

	return tpr[0].ProposalResponse.GetResponse().Payload, tpr, nil
}

func getTxProposal(reqCtx reqContext.Context, request fab.ChaincodeInvokeRequest, channelID string) (*fab.TransactionProposal, error) {

	ctx, ok := contextImpl.RequestClientContext(reqCtx)
	if !ok {
		return nil, errors.New("failed get client context from reqContext for txn header")
	}

	txh, err := txn.NewHeader(ctx, channelID)
	if err != nil {
		return nil, errors.WithMessage(err, "create transaction ID failed")
	}

	prop, err := txn.CreateChaincodeInvokeProposal(txh, request)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get tx proposal")
	}
	return prop, nil
}

// block section ...

func retrieveBlockPayload(reqCtx reqContext.Context, channel string, pos *ab.SeekPosition) (*common.Payload, error) {
	ctx, ok := contextImpl.RequestClientContext(reqCtx)
	if !ok {
		return nil, errors.New("failed get client context from reqContext for signPayload")
	}
	th, err := txn.NewHeader(ctx, channel)
	if err != nil {
		return nil, errors.Wrap(err, "generating TX ID failed")
	}

	hash, err := ccomm.TLSCertHash(ctx.EndpointConfig())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get tls cert hash")
	}

	channelHeaderOpts := txn.ChannelHeaderOpts{
		TxnHeader:   th,
		TLSCertHash: hash,
	}

	seekInfoHeader, err := txn.CreateChannelHeader(common.HeaderType_DELIVER_SEEK_INFO, channelHeaderOpts)
	if err != nil {
		return nil, errors.Wrap(err, "CreateChannelHeader failed")
	}

	seekInfoHeaderBytes, err := proto.Marshal(seekInfoHeader)
	if err != nil {
		return nil, errors.Wrap(err, "marshal seek info failed")
	}

	signatureHeader, err := txn.CreateSignatureHeader(th)
	if err != nil {
		return nil, errors.Wrap(err, "CreateSignatureHeader failed")
	}

	signatureHeaderBytes, err := proto.Marshal(signatureHeader)
	if err != nil {
		return nil, errors.Wrap(err, "marshal signature header failed")
	}

	seekHeader := &common.Header{
		ChannelHeader:   seekInfoHeaderBytes,
		SignatureHeader: signatureHeaderBytes,
	}

	seekInfo := &ab.SeekInfo{
		Start:    pos,
		Stop:     pos,
		Behavior: ab.SeekInfo_BLOCK_UNTIL_READY,
	}

	seekInfoBytes, err := proto.Marshal(seekInfo)
	if err != nil {
		return nil, errors.Wrap(err, "marshal seek info failed")
	}

	payload := common.Payload{
		Header: seekHeader,
		Data:   seekInfoBytes,
	}
	return &payload, err
}

// GetConfigBlock retrieves the block using signed envelope
func GetConfigBlock(reqCtx reqContext.Context, orderers []fab.Orderer, envelope *fab.SignedEnvelope, opts ...Opt) (*common.Block, error) {
	optionsValue := getOpts(opts...)

	resp, err := retry.NewInvoker(retry.New(optionsValue.retry)).Invoke(
		func() (interface{}, error) {
			return txn.GetConfigBlock(reqCtx, envelope, orderers)
		},
	)
	if err != nil {
		return nil, err
	}
	return resp.(*common.Block), err
}

// lscc section ...

const (
	lsccChaincodes = "getchaincodes"
)

func createChaincodeInvokeRequest() fab.ChaincodeInvokeRequest {
	cir := fab.ChaincodeInvokeRequest{
		ChaincodeID: lscc,
		Fcn:         lsccChaincodes,
	}
	return cir
}
