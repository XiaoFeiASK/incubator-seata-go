/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package client

import (
	"context"
	"errors"

	"seata.apache.org/seata-go/v2/pkg/protocol"
	"seata.apache.org/seata-go/v2/pkg/protocol/branch"
	"seata.apache.org/seata-go/v2/pkg/protocol/message"
	"seata.apache.org/seata-go/v2/pkg/remoting/grpc/pb"
	"seata.apache.org/seata-go/v2/pkg/util/log"
	"seata.apache.org/seata-go/v2/pkg/util/reflectx"

	"seata.apache.org/seata-go/v2/pkg/remoting/config"
	"seata.apache.org/seata-go/v2/pkg/remoting/getty"
	"seata.apache.org/seata-go/v2/pkg/remoting/grpc"
	"seata.apache.org/seata-go/v2/pkg/rm"
)

func initBranchCommit() {
	rmBranchCommitProcessor := &rmBranchCommitProcessor{}
	switch protocol.Protocol(config.GetTransportConfig().Protocol) {
	case protocol.ProtocolGRPC:
		grpc.GetGrpcClientHandlerInstance().RegisterType(reflectx.ProtoMessageName[*pb.BranchCommitRequestProto](), message.MessageTypeBranchCommit)

		grpc.GetGrpcClientHandlerInstance().RegisterProcessor(message.MessageTypeBranchCommit, rmBranchCommitProcessor)
	default:
		getty.GetGettyClientHandlerInstance().RegisterProcessor(message.MessageTypeBranchCommit, rmBranchCommitProcessor)
	}
}

type rmBranchCommitProcessor struct {
	getResourceManager func(branch.BranchType) rm.ResourceManager
	sendGrpcResponse   func(int32, interface{}) error
	sendGettyResponse  func(int32, interface{}) error
}

// branchEndResult is the protocol-independent result of a branch operation.
type branchEndResult struct {
	status     branch.BranchStatus
	resultCode message.ResultCode
	errMsg     string
}

func newBranchEndResult(status branch.BranchStatus, bizErr error) branchEndResult {
	result := branchEndResult{status: status, resultCode: message.ResultCodeSuccess}
	if bizErr != nil {
		result.resultCode = message.ResultCodeFailed
		result.errMsg = bizErr.Error()
	}
	return result
}

func branchEndProcessError(bizErr, sendErr error) error {
	if sendErr == nil {
		return nil
	}
	if bizErr == nil {
		return sendErr
	}
	return errors.Join(bizErr, sendErr)
}

func (f *rmBranchCommitProcessor) resourceManager(branchType branch.BranchType) rm.ResourceManager {
	if f.getResourceManager != nil {
		return f.getResourceManager(branchType)
	}
	return rm.GetRmCacheInstance().GetResourceManager(branchType)
}

func (f *rmBranchCommitProcessor) Process(ctx context.Context, rpcMessage message.RpcMessage) error {
	log.Infof("the rm client received  rmBranchCommit rpcMessage %#v from tc server.", rpcMessage)
	switch protocol.Protocol(config.GetTransportConfig().Protocol) {
	case protocol.ProtocolGRPC:
		return f.handleGrpcBranchCommit(ctx, rpcMessage)
	default:
		return f.handleGettyBranchCommit(ctx, rpcMessage)
	}
}

func (f *rmBranchCommitProcessor) handleGrpcBranchCommit(ctx context.Context, rpcMessage message.RpcMessage) error {
	request := rpcMessage.Body.(*pb.BranchCommitRequestProto)
	xid := request.AbstractBranchEndRequest.Xid
	branchID := request.AbstractBranchEndRequest.BranchId
	resourceID := request.AbstractBranchEndRequest.ResourceId
	applicationData := request.AbstractBranchEndRequest.ApplicationData
	log.Infof("Branch committing: xid %s, branchID %d, resourceID %s, applicationData %s", xid, branchID, resourceID, applicationData)
	branchResource := rm.BranchResource{
		ResourceId:      resourceID,
		BranchId:        branchID,
		ApplicationData: []byte(applicationData),
		Xid:             xid,
	}

	status, bizErr := f.resourceManager(branch.BranchType(request.AbstractBranchEndRequest.BranchType)).BranchCommit(ctx, branchResource)
	if bizErr != nil {
		log.Errorf("branch commit error: %s", bizErr.Error())
	} else {
		log.Infof("branch commit success: xid %s, branchID %d, resourceID %s, applicationData %s", xid, branchID, resourceID, applicationData)
	}
	result := newBranchEndResult(status, bizErr)

	// reply commit response to tc server
	// todo add TransactionErrorCode
	response := &pb.BranchCommitResponseProto{
		AbstractBranchEndResponse: &pb.AbstractBranchEndResponseProto{
			AbstractTransactionResponse: &pb.AbstractTransactionResponseProto{
				AbstractResultMessage: &pb.AbstractResultMessageProto{
					ResultCode: pb.ResultCodeProto(result.resultCode),
					Msg:        result.errMsg,
				},
			},
			Xid:          xid,
			BranchId:     branchID,
			BranchStatus: pb.BranchStatusProto(result.status),
		},
	}

	sendResponse := f.sendGrpcResponse
	if sendResponse == nil {
		sendResponse = grpc.GetGrpcRemotingClient().SendAsyncResponse
	}
	sendErr := sendResponse(rpcMessage.ID, response)
	if sendErr != nil {
		log.Errorf("send branch commit response error: {%#v}", sendErr.Error())
	} else {
		log.Infof("send branch commit response success: xid %s, branchID %v, resourceID %v, applicationData %v", xid, branchID, resourceID, applicationData)
	}
	return branchEndProcessError(bizErr, sendErr)
}

func (f *rmBranchCommitProcessor) handleGettyBranchCommit(ctx context.Context, rpcMessage message.RpcMessage) error {
	request := rpcMessage.Body.(message.BranchCommitRequest)
	xid := request.Xid
	branchID := request.BranchId
	resourceID := request.ResourceId
	applicationData := request.ApplicationData
	log.Infof("Branch committing: xid %s, branchID %d, resourceID %s, applicationData %s", xid, branchID, resourceID, applicationData)
	branchResource := rm.BranchResource{
		ResourceId:      resourceID,
		BranchId:        branchID,
		ApplicationData: applicationData,
		Xid:             xid,
	}

	status, bizErr := f.resourceManager(request.BranchType).BranchCommit(ctx, branchResource)
	if bizErr != nil {
		log.Errorf("branch commit error: %s", bizErr.Error())
	} else {
		log.Infof("branch commit success: xid %s, branchID %d, resourceID %s, applicationData %s", xid, branchID, resourceID, applicationData)
	}
	result := newBranchEndResult(status, bizErr)

	// reply commit response to tc server
	// todo add TransactionErrorCode
	response := message.BranchCommitResponse{
		AbstractBranchEndResponse: message.AbstractBranchEndResponse{
			AbstractTransactionResponse: message.AbstractTransactionResponse{
				AbstractResultMessage: message.AbstractResultMessage{
					ResultCode: result.resultCode,
					Msg:        result.errMsg,
				},
			},
			Xid:          xid,
			BranchId:     branchID,
			BranchStatus: result.status,
		},
	}
	sendResponse := f.sendGettyResponse
	if sendResponse == nil {
		sendResponse = getty.GetGettyRemotingClient().SendAsyncResponse
	}
	sendErr := sendResponse(rpcMessage.ID, response)
	if sendErr != nil {
		log.Errorf("send branch commit response error: {%#v}", sendErr.Error())
	} else {
		log.Infof("send branch commit response success: xid %s, branchID %v, resourceID %v, applicationData %v", xid, branchID, resourceID, applicationData)
	}
	return branchEndProcessError(bizErr, sendErr)
}
