/*
 *
 *  * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 *  * this file except in compliance with the License. You may obtain a copy of the
 *  * License at  http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed
 *  * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *  * CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package gobblin.couchbase.writer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.transcoder.Transcoder;
import com.typesafe.config.Config;

import gobblin.couchbase.common.TupleDocument;
import gobblin.instrumented.writer.InstrumentedDataWriter;
import gobblin.util.ConfigUtils;



public class CouchbaseWriter extends InstrumentedDataWriter<TupleDocument> {

  private final Cluster _cluster;
  private final Bucket _bucket;
  private Long _recordsWritten = 0L;

  private final Transcoder<TupleDocument, Tuple2<ByteBuf, Integer>> _defaultTranscoder =
      new Transcoder<TupleDocument, Tuple2<ByteBuf, Integer>>()
      {
        @Override
        public TupleDocument decode(String id, ByteBuf content, long cas, int expiry, int flags,
            ResponseStatus status)
        {
          return newDocument(id, expiry, Tuple.create(content, flags), cas);
        }

        @Override
        public Tuple2<ByteBuf, Integer> encode(TupleDocument document)
        {
          return document.content();
        }

        @Override
        public TupleDocument newDocument(String id, int expiry, Tuple2<ByteBuf, Integer> content,
            long cas)
        {
          return new TupleDocument(id, expiry, content, cas);
        }

        @Override
        public TupleDocument newDocument(String id, int expiry, Tuple2<ByteBuf, Integer> content,
            long cas, MutationToken mutationToken)
        {
          return new TupleDocument(id, expiry, content, cas);
        }

        @Override
        public Class<TupleDocument> documentType()
        {
          return TupleDocument.class;
        }
      };

  public CouchbaseWriter(Config config) {
    super(ConfigUtils.configToState(config));
    List<String> bootstrapServers = ConfigUtils.getStringList(config,
        CouchbaseWriterConfigurationKeys.BOOTSTRAP_SERVERS);

    if (bootstrapServers.isEmpty()) {
      bootstrapServers = CouchbaseWriterConfigurationKeys.BOOTSTRAP_SERVERS_DEFAULT;
    }

    _cluster = CouchbaseCluster.create(bootstrapServers);

    String bucketName = ConfigUtils.getString(config, CouchbaseWriterConfigurationKeys.BUCKET, null);



    if (bucketName == null)
    {
      // throw instantiation exception since we need a valid bucket name
      throw new RuntimeException("Need a valid bucket name");
    }

    String password = "";
    _bucket = _cluster.openBucket(bucketName, password,
        Collections.<Transcoder<? extends Document, ?>>singletonList(_defaultTranscoder));

  }

  @Override
  public void cleanup()
      throws IOException {

  }

  @Override
  public long recordsWritten() {
    return _recordsWritten;
  }

  @Override
  public long bytesWritten()
      throws IOException {
    return 0;
  }

  @Override
  public void writeImpl(TupleDocument record)
      throws IOException {

    try {
      _bucket.upsert(record);
      _recordsWritten++;
    } catch (Exception e) {
      throw new IOException("Failed to write to couchbase cluster", e);
    }
  }


  @Override
  public void close() {
    _bucket.close();
    _cluster.disconnect();
  }

}
