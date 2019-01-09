/*
Copyright Kompitech Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package resmgmt

import (
	reqContext "context"
	"os"

	"github.com/hyperledger/fabric-sdk-go/pkg/common/providers/fab"
	"github.com/hyperledger/fabric-sdk-go/third_party/github.com/hyperledger/fabric/protos/common"

	clientCh "github.com/hyperledger/fabric-sdk-go/pkg/client/channel"
	"github.com/hyperledger/fabric-sdk-go/pkg/common/errors/multi"
	"github.com/hyperledger/fabric-sdk-go/pkg/common/errors/status"
	contextImpl "github.com/hyperledger/fabric-sdk-go/pkg/context"
	"github.com/hyperledger/fabric-sdk-go/pkg/fab/peer"
	"github.com/hyperledger/fabric-sdk-go/pkg/fab/resource"
	"github.com/hyperledger/fabric-sdk-go/pkg/fab/txn"
	pb "github.com/hyperledger/fabric-sdk-go/third_party/github.com/hyperledger/fabric/protos/peer"
	"github.com/pkg/errors"
)

// SendJoinChannelProposal joins peers to channel using signed proposal
func (rc *Client) SendJoinChannelProposal(signedProposal *pb.SignedProposal, options ...RequestOption) error {

	opts, err := rc.prepareRequestOpts(options...)
	if err != nil {
		return errors.WithMessage(err, "failed to get opts for JoinChannel")
	}

	//resolve timeouts
	rc.resolveTimeouts(&opts)

	//set parent request context for overall timeout
	parentReqCtx, parentReqCancel := contextImpl.NewRequest(rc.ctx, contextImpl.WithTimeout(opts.Timeouts[fab.ResMgmt]), contextImpl.WithParent(opts.ParentContext))
	parentReqCtx = reqContext.WithValue(parentReqCtx, contextImpl.ReqContextTimeoutOverrides, opts.Timeouts)
	defer parentReqCancel()

	targets, err := rc.calculateTargets(opts.Targets, opts.TargetFilter)
	if err != nil {
		return errors.WithMessage(err, "failed to determine target peers for JoinChannel")
	}

	if len(targets) == 0 {
		return errors.WithStack(status.New(status.ClientStatus, status.NoPeersFound.ToInt32(), "no targets available", nil))
	}

	peerReqCtx, peerReqCtxCancel := contextImpl.NewRequest(rc.ctx, contextImpl.WithTimeoutType(fab.ResMgmt), contextImpl.WithParent(parentReqCtx))
	defer peerReqCtxCancel()

	err = resource.SendJoinChannelProposal(peerReqCtx, peersToTxnProcessors(targets), signedProposal, resource.WithRetry(opts.Retry))
	if err != nil {
		return errors.WithMessage(err, "join channel failed")
	}

	return nil
}

// GetJoinChannelTxProposal returns join channel tx proposal
func (rc *Client) GetJoinChannelTxProposal(genesisBlock *common.Block, options ...RequestOption) (*fab.TransactionProposal, error) {

	opts, err := rc.prepareRequestOpts(options...)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get opts when getting join channel proposal")
	}
	//resolve timeouts
	rc.resolveTimeouts(&opts)

	//set parent request context for overall timeout
	parentReqCtx, parentReqCancel := contextImpl.NewRequest(rc.ctx, contextImpl.WithTimeout(opts.Timeouts[fab.ResMgmt]), contextImpl.WithParent(opts.ParentContext))
	parentReqCtx = reqContext.WithValue(parentReqCtx, contextImpl.ReqContextTimeoutOverrides, opts.Timeouts)
	defer parentReqCancel()

	joinChannelRequest := resource.JoinChannelRequest{
		GenesisBlock: genesisBlock,
	}

	peerReqCtx, peerReqCtxCancel := contextImpl.NewRequest(rc.ctx, contextImpl.WithTimeoutType(fab.ResMgmt), contextImpl.WithParent(parentReqCtx))
	defer peerReqCtxCancel()

	tp, err := resource.GetJoinChannelTxProposal(peerReqCtx, joinChannelRequest)
	if err != nil {
		return nil, errors.WithMessage(err, "get join channel transaction proposal failed")
	}

	return tp, nil
}

// GetConfigBlockPayload returns config block payload
func (rc *Client) GetConfigBlockPayload(channelID string, position string, index uint64, options ...RequestOption) (*common.Payload, error) {

	if channelID == "" {
		return nil, errors.New("must provide channel ID")
	}

	opts, err := rc.prepareRequestOpts(options...)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get opts while getting config block payload")
	}

	//resolve timeouts
	rc.resolveTimeouts(&opts)

	//set parent request context for overall timeout
	parentReqCtx, parentReqCancel := contextImpl.NewRequest(rc.ctx, contextImpl.WithTimeout(opts.Timeouts[fab.ResMgmt]), contextImpl.WithParent(opts.ParentContext))
	parentReqCtx = reqContext.WithValue(parentReqCtx, contextImpl.ReqContextTimeoutOverrides, opts.Timeouts)
	defer parentReqCancel()

	ordrReqCtx, ordrReqCtxCancel := contextImpl.NewRequest(rc.ctx, contextImpl.WithTimeoutType(fab.OrdererResponse), contextImpl.WithParent(parentReqCtx))
	defer ordrReqCtxCancel()

	payload, err := resource.GetBlockPayload(ordrReqCtx, channelID, position, index)
	if err != nil {
		return nil, errors.WithMessage(err, "config block payload retrieval failed")
	}
	return payload, nil
}

// GetConfigBlock returns configuration block
func (rc *Client) GetConfigBlock(channelID string, envelope *fab.SignedEnvelope, options ...RequestOption) (*common.Block, error) {

	if channelID == "" {
		return nil, errors.New("must provide channel ID")
	}

	opts, err := rc.prepareRequestOpts(options...)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get opts while getting config block")
	}

	//resolve timeouts
	rc.resolveTimeouts(&opts)

	//set parent request context for overall timeout
	parentReqCtx, parentReqCancel := contextImpl.NewRequest(rc.ctx, contextImpl.WithTimeout(opts.Timeouts[fab.ResMgmt]), contextImpl.WithParent(opts.ParentContext))
	parentReqCtx = reqContext.WithValue(parentReqCtx, contextImpl.ReqContextTimeoutOverrides, opts.Timeouts)
	defer parentReqCancel()

	orderer, err := rc.requestOrderer(&opts, channelID)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to find orderer for request")
	}

	ordrReqCtx, ordrReqCtxCancel := contextImpl.NewRequest(rc.ctx, contextImpl.WithTimeoutType(fab.OrdererResponse), contextImpl.WithParent(parentReqCtx))
	defer ordrReqCtxCancel()

	block, err := resource.GetConfigBlock(ordrReqCtx, []fab.Orderer{orderer}, envelope, resource.WithRetry(opts.Retry))
	if err != nil {
		return nil, errors.WithMessage(err, "config block retrieval failed")
	}
	return block, nil
}

// GetCCInstallPayload returns install CC tx proposal
func (rc *Client) GetCCInstallPayload(req InstallCCRequest) (*fab.TransactionProposal, error) {
	// For each peer query if chaincode installed. If cc is installed treat as success with message 'already installed'.
	// If cc is not installed try to install, and if that fails add to the list with error and peer name.

	err := checkRequiredInstallCCParams(req)
	if err != nil {
		return nil, err
	}

	reqCtx, cancel := contextImpl.NewRequest(rc.ctx)
	defer cancel()

	icr := resource.InstallChaincodeRequest{Name: req.Name, Path: req.Path, Version: req.Version, Package: req.Package}
	return resource.GetCCInstallProposal(reqCtx, icr)
}

// SendCCInstallProposal allows administrators to install chaincode onto the filesystem of a peer using signed proposal
func (rc *Client) SendCCInstallProposal(req InstallCCRequest, signedProposal *pb.SignedProposal, options ...RequestOption) ([]InstallCCResponse, error) {
	// For each peer query if chaincode installed. If cc is installed treat as success with message 'already installed'.
	// If cc is not installed try to install, and if that fails add to the list with error and peer name.

	err := checkRequiredInstallCCParams(req)
	if err != nil {
		return nil, err
	}

	opts, err := rc.prepareRequestOpts(options...)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get opts while sending install CC proposal")
	}

	//resolve timeouts
	rc.resolveTimeouts(&opts)

	//set parent request context for overall timeout
	parentReqCtx, parentReqCancel := contextImpl.NewRequest(rc.ctx, contextImpl.WithTimeout(opts.Timeouts[fab.ResMgmt]), contextImpl.WithParent(opts.ParentContext))
	parentReqCtx = reqContext.WithValue(parentReqCtx, contextImpl.ReqContextTimeoutOverrides, opts.Timeouts)
	defer parentReqCancel()

	//Default targets when targets are not provided in options
	defaultTargets, err := rc.resolveDefaultTargets(&opts)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get default targets while sending install CC proposal")
	}

	targets, err := rc.calculateTargets(defaultTargets, opts.TargetFilter)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to determine target peers while sending install CC proposal")
	}

	if len(targets) == 0 {
		return nil, errors.WithStack(status.New(status.ClientStatus, status.NoPeersFound.ToInt32(), "no targets available", nil))
	}

	responses := []InstallCCResponse{}
	newTargets := targets
	errs := multi.Errors{}
	// TODO build newtargets

	// responses, newTargets, errs := rc.adjustTargets(targets, req, opts.Retry, parentReqCtx)

	// if len(newTargets) == 0 {
	// 	// CC is already installed on all targets and/or
	// 	// we are unable to verify if cc is installed on target(s)
	// 	return responses, errs.ToError()
	// }

	reqCtx, cancel := contextImpl.NewRequest(rc.ctx, contextImpl.WithTimeoutType(fab.ResMgmt), contextImpl.WithParent(parentReqCtx))
	defer cancel()

	responses, err = rc.sendInstallCCProposal(req, reqCtx, newTargets, responses, signedProposal)

	if err != nil {
		installErrs, ok := err.(multi.Errors)
		if ok {
			errs = append(errs, installErrs)
		} else {
			errs = append(errs, err)
		}
	}

	return responses, errs.ToError()
}

func (rc *Client) sendInstallCCProposal(req InstallCCRequest, reqCtx reqContext.Context, newTargets []fab.Peer, responses []InstallCCResponse, signedProposal *pb.SignedProposal) ([]InstallCCResponse, error) {
	_, transactionProposalResponse, err := resource.QueryChannel(reqCtx, peer.PeersToTxnProcessors(newTargets), signedProposal)
	if err != nil {
		return nil, errors.WithMessage(err, "installing chaincode failed")
	}

	for _, v := range transactionProposalResponse {
		logger.Debugf("Install chaincode '%s' endorser '%s' returned ProposalResponse status:%v", req.Name, v.Endorser, v.Status)

		response := InstallCCResponse{Target: v.Endorser, Status: v.Status}
		responses = append(responses, response)
	}
	return responses, nil
}

// SendTxCCProposal sends signed tx proposal
func (rc *Client) SendTxCCProposal(signedProposal *pb.SignedProposal, proposal *fab.TransactionProposal, options ...RequestOption) (*common.Payload, *clientCh.Response, error) {

	opts, err := rc.prepareRequestOpts(options...)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "failed to get opts for while sending initial endorse CC proposal")
	}

	reqCtx, cancel := rc.createRequestContext(opts, fab.ResMgmt)
	defer cancel()

	payload, txResponse, err := rc.sendTxCCProposal(reqCtx, opts, signedProposal, proposal)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "failed to get payload while sending initial endorse CC proposal")
	}
	return payload, txResponse, nil
}

// GetExistingCC queries the installed chaincodes on a peer.
func (rc *Client) GetExistingCC(signedProposal *pb.SignedProposal, options ...RequestOption) (*pb.ChaincodeQueryResponse, error) {

	opts, err := rc.prepareRequestOpts(options...)
	if err != nil {
		return nil, err
	}

	if len(opts.Targets) != 1 {
		return nil, errors.New("only one target is supported")
	}

	reqCtx, cancel := rc.createRequestContext(opts, fab.PeerResponse)
	defer cancel()

	return resource.GetExistingCC(reqCtx, opts.Targets[0], signedProposal, resource.WithRetry(opts.Retry))
}

// GetJoinedChannels queries the names of all the channels that a peer has joined.
func (rc *Client) GetJoinedChannels(signedProposal *pb.SignedProposal, options ...RequestOption) (*pb.ChannelQueryResponse, error) {

	opts, err := rc.prepareRequestOpts(options...)
	if err != nil {
		return nil, err
	}

	if len(opts.Targets) != 1 {
		return nil, errors.New("only one target is supported")
	}

	reqCtx, cancel := rc.createRequestContext(opts, fab.PeerResponse)
	defer cancel()

	return resource.GetJoinedChannels(reqCtx, opts.Targets[0], signedProposal, resource.WithRetry(opts.Retry))
}

// GetQueryChannelsPayload returns tx proposal to to get peer's joined channels
func (rc *Client) GetQueryChannelsPayload() (*fab.TransactionProposal, error) {

	reqCtx, cancel := contextImpl.NewRequest(rc.ctx)
	defer cancel()

	return resource.GetQueryChannelsPayload(reqCtx)
}

// sendCCProposal sends signed tx proposal
func (rc *Client) sendTxCCProposal(reqCtx reqContext.Context, opts requestOptions, signedProposal *pb.SignedProposal, proposal *fab.TransactionProposal) (*common.Payload, *clientCh.Response, error) {

	//Default targets when targets are not provided in options
	targets := []fab.Peer{}
	if len(opts.Targets) == 0 {
		defaultTargets, err := rc.resolveDefaultTargets(&opts)
		if err != nil {
			return nil, nil, errors.WithMessage(err, "failed to get default targets while sending initial endorse CC proposal")
		}

		targets, err = rc.calculateTargets(defaultTargets, opts.TargetFilter)
		if err != nil {
			return nil, nil, errors.WithMessage(err, "failed to determine target peers while sending initial endorse CC proposal")
		}
	} else {
		targets = opts.Targets
	}

	if len(targets) == 0 {
		return nil, nil, errors.WithStack(status.New(status.ClientStatus, status.NoPeersFound.ToInt32(), "no targets available", nil))
	}

	txProposalResponses, err := txn.SendProposalCustom(reqCtx, peersToTxnProcessors(targets), signedProposal)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "sending initial endorse transaction proposal failed")
	}
	// Verify signature(s)
	// err = rc.verifyTPSignature(channelService, txProposalResponse)
	// if err != nil {
	// 	return nil, errors.WithMessage(err, "sending deploy transaction proposal failed to verify signature")
	// }

	transactionRequest := fab.TransactionRequest{
		Proposal:          proposal,
		ProposalResponses: txProposalResponses,
	}

	// send transaction and check event
	return createTxProposal(transactionRequest)
}

func createTxProposal(request fab.TransactionRequest) (*common.Payload, *clientCh.Response, error) {
	tx, err := txn.New(request)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "creating new tx proposal failed")
	}
	txResponse := clientCh.Response{}
	if len(request.ProposalResponses) > 0 {
		txResponse.Payload = request.ProposalResponses[0].ProposalResponse.GetResponse().Payload
	}

	payload, err := txn.CreateTxProposal(tx)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "creating tx proposal failed")

	}

	return payload, &txResponse, nil
}

// GetSaveChannelPayload returns payload to create or update channel
func (rc *Client) GetSaveChannelPayload(req SaveChannelRequest, options ...RequestOption) (*common.Payload, error) {

	opts, err := rc.prepareRequestOpts(options...)
	if err != nil {
		return nil, err
	}

	if req.ChannelConfigPath != "" {
		configReader, err1 := os.Open(req.ChannelConfigPath)
		if err1 != nil {
			return nil, errors.Wrapf(err1, "opening channel config file failed")
		}
		defer loggedClose(configReader)
		req.ChannelConfig = configReader
	}

	err = rc.validateSaveChannelRequest(req)
	if err != nil {
		return nil, errors.WithMessage(err, "reading channel config file failed")
	}

	logger.Debugf("saving channel: %s", req.ChannelID)

	chConfig, err := extractChConfigData(req.ChannelConfig)
	if err != nil {
		return nil, errors.WithMessage(err, "extracting channel config from ConfigTx failed")
	}

	orderer, err := rc.requestOrderer(&opts, req.ChannelID)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to find orderer for request")
	}

	var configSignatures []*common.ConfigSignature
	if opts.Signatures != nil {
		configSignatures = opts.Signatures
	} else {
		configSignatures, err = rc.getConfigSignatures(req, chConfig)
		if err != nil {
			return nil, err
		}
	}

	request := resource.CreateChannelRequest{
		Name:       req.ChannelID,
		Orderer:    orderer,
		Config:     chConfig,
		Signatures: configSignatures,
	}

	reqCtx, cancel := rc.createRequestContext(opts, fab.OrdererResponse)
	defer cancel()

	payload, err := resource.GetSaveChannelPayload(reqCtx, request, resource.WithRetry(opts.Retry))
	if err != nil {
		return nil, errors.WithMessage(err, "create channel failed")
	}

	return payload, nil
}

// SendSignedPayload sends signed payload
func (rc *Client) SendSignedPayload(channelID string, envelope *fab.SignedEnvelope, options ...RequestOption) (*fab.TransactionResponse, error) {

	opts, err := rc.prepareRequestOpts(options...)
	if err != nil {
		return nil, err
	}

	orderer, err := rc.requestOrderer(&opts, channelID)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to find orderer for request")
	}

	reqCtx, cancel := rc.createRequestContext(opts, fab.OrdererResponse)
	defer cancel()

	resp, err := resource.BroadcastSignedPayload(reqCtx, orderer, envelope)
	if err != nil {
		return nil, errors.WithMessage(err, "broadcasting signed payload failed")
	}

	return resp, nil
}

// GetCCInstantiateProposal returns tx proposal to instantiate CC
func (rc *Client) GetCCInstantiateProposal(ccProposalType chaincodeProposalType, channelID string, req InstantiateCCRequest) (*fab.TransactionProposal, fab.TransactionID, error) {
	if err := checkRequiredCCProposalParams(channelID, req); err != nil {
		return nil, fab.EmptyTransactionID, err
	}

	// create a transaction proposal for chaincode deployment
	tp, txnID, err := rc.createTP(req, channelID, ccProposalType)
	if err != nil {
		return nil, txnID, err
	}
	return tp, tp.TxnID, err
}

// GetExistingCCProposal returns tx proposal for installed or instantiated CC
func (rc *Client) GetExistingCCProposal(queryType, channelID string) (*fab.TransactionProposal, error) {

	reqCtx, cancel := contextImpl.NewRequest(rc.ctx)
	defer cancel()

	return resource.GetExistingCCProposal(reqCtx, queryType, channelID)
}

// GetExistingCCProposal returns tx proposal for CC invoke or query
func (rc *Client) CreateInvokeCCProposal(chrequest *clientCh.Request, channelID string) (*fab.TransactionProposal, error) {
	request := fab.ChaincodeInvokeRequest{
		ChaincodeID:  chrequest.ChaincodeID,
		Fcn:          chrequest.Fcn,
		Args:         chrequest.Args,
		TransientMap: chrequest.TransientMap,
	}

	txh, err := txn.NewHeader(rc.ctx, channelID)
	if err != nil {
		return nil, errors.WithMessage(err, "creating transaction header failed")
	}

	proposal, err := txn.CreateChaincodeInvokeProposal(txh, request)
	if err != nil {
		return nil, errors.WithMessage(err, "creating invoke CC transaction proposal failed")
	}
	return proposal, err
}
