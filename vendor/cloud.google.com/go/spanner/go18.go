// Copyright 2017 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build go1.8

package spanner

import (
	"fmt"

	ocgrpc "go.opencensus.io/plugin/grpc"
	"go.opencensus.io/trace"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func openCensusOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithGRPCDialOption(grpc.WithStatsHandler(ocgrpc.NewClientStatsHandler())),
	}
}

func traceStartSpan(ctx context.Context, name string) context.Context {
	return trace.StartSpan(ctx, name)
}

func traceEndSpan(ctx context.Context, err error) {
	span := trace.FromContext(ctx)
	if err != nil {
		span.SetStatus(trace.Status{Message: err.Error()})
	}
	trace.EndSpan(ctx)
}

func tracePrintf(ctx context.Context, attrMap map[string]interface{}, format string, args ...interface{}) {
	var attrs []trace.Attribute
	for k, v := range attrMap {
		var a trace.Attribute
		switch v := v.(type) {
		case string:
			a = trace.StringAttribute{k, v}
		case bool:
			a = trace.BoolAttribute{k, v}
		case int:
			a = trace.Int64Attribute{k, int64(v)}
		case int64:
			a = trace.Int64Attribute{k, v}
		default:
			a = trace.StringAttribute{k, fmt.Sprintf("%#v", v)}
		}
		attrs = append(attrs, a)
	}
	trace.FromContext(ctx).Annotatef(attrs, format, args...)
}
