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

package codec

import (
	"testing"

	"github.com/stretchr/testify/assert"

	model2 "seata.apache.org/seata-go/v2/pkg/protocol/branch"
	"seata.apache.org/seata-go/v2/pkg/protocol/message"
	serror "seata.apache.org/seata-go/v2/pkg/util/errors"
)

func TestBranchCommitResponseCodec(t *testing.T) {
	msg := message.BranchCommitResponse{
		AbstractBranchEndResponse: message.AbstractBranchEndResponse{
			Xid:          "123344",
			BranchId:     56678,
			BranchStatus: model2.BranchStatusPhaseoneFailed,
			AbstractTransactionResponse: message.AbstractTransactionResponse{
				TransactionErrorCode: serror.TransactionErrorCodeBeginFailed,
				AbstractResultMessage: message.AbstractResultMessage{
					ResultCode: message.ResultCodeFailed,
					Msg:        "FAILED",
				},
			},
		},
	}

	codec := BranchCommitResponseCodec{}
	bytes := codec.Encode(msg)
	msg2 := codec.Decode(bytes)

	assert.Equal(t, msg, msg2)
}

// Byte vector derived from Java AbstractResultMessageCodec,
// AbstractTransactionResponseCodec, and AbstractBranchEndResponseCodec.
func TestBranchCommitResponseCodec_JavaWireFormat(t *testing.T) {
	msg := message.BranchCommitResponse{
		AbstractBranchEndResponse: message.AbstractBranchEndResponse{
			Xid:          "192.168.0.1:8091:1234",
			BranchId:     5678,
			BranchStatus: model2.BranchStatusPhasetwoCommitFailedRetryable,
			AbstractTransactionResponse: message.AbstractTransactionResponse{
				TransactionErrorCode: serror.TransactionErrorCodeUnknown,
				AbstractResultMessage: message.AbstractResultMessage{
					ResultCode: message.ResultCodeFailed,
					Msg:        "storage failed",
				},
			},
		},
	}
	want := []byte{
		0x00,
		0x00, 0x0e,
		's', 't', 'o', 'r', 'a', 'g', 'e', ' ', 'f', 'a', 'i', 'l', 'e', 'd',
		0x00,
		0x00, 0x15,
		'1', '9', '2', '.', '1', '6', '8', '.', '0', '.', '1', ':', '8', '0', '9', '1', ':', '1', '2', '3', '4',
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x16, 0x2e,
		0x06,
	}

	codec := BranchCommitResponseCodec{}
	assert.Equal(t, want, codec.Encode(msg), "encode must reproduce the Java bytes exactly")
	assert.Equal(t, msg, codec.Decode(want))
}
