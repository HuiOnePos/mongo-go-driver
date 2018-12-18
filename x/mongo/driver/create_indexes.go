// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package driver

import (
	"context"

	"time"

	"mongo-go-driver/mongo/options"
	"mongo-go-driver/x/bsonx"
	"mongo-go-driver/x/mongo/driver/session"
	"mongo-go-driver/x/mongo/driver/topology"
	"mongo-go-driver/x/mongo/driver/uuid"
	"mongo-go-driver/x/network/command"
	"mongo-go-driver/x/network/description"
	"mongo-go-driver/x/network/result"
)

// CreateIndexes handles the full cycle dispatch and execution of a createIndexes
// command against the provided topology.
func CreateIndexes(
	ctx context.Context,
	cmd command.CreateIndexes,
	topo *topology.Topology,
	selector description.ServerSelector,
	clientID uuid.UUID,
	pool *session.Pool,
	opts ...*options.CreateIndexesOptions,
) (result.CreateIndexes, error) {

	ss, err := topo.SelectServer(ctx, selector)
	if err != nil {
		return result.CreateIndexes{}, err
	}

	conn, err := ss.Connection(ctx)
	if err != nil {
		return result.CreateIndexes{}, err
	}
	defer conn.Close()

	cio := options.MergeCreateIndexesOptions(opts...)
	if cio.MaxTime != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"maxTimeMS", bsonx.Int64(int64(*cio.MaxTime / time.Millisecond))})
	}

	// If no explicit session and deployment supports sessions, start implicit session.
	if cmd.Session == nil && topo.SupportsSessions() {
		cmd.Session, err = session.NewClientSession(pool, clientID, session.Implicit)
		if err != nil {
			return result.CreateIndexes{}, err
		}
		defer cmd.Session.EndSession()
	}

	return cmd.RoundTrip(ctx, ss.Description(), conn)
}
