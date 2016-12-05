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
import java.util.List;

import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.transcoder.Transcoder;
import com.typesafe.config.Config;

import gobblin.instrumented.writer.InstrumentedDataWriter;
import gobblin.util.ConfigUtils;



public class CouchbaseWriter<D extends KeyValueRecord<String, V>, V> extends InstrumentedDataWriter<D> {

  private final Cluster _cluster;
  private final Bucket _bucket;
  private Long _recordsWritten = 0L;


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

    _bucket = _cluster.openBucket(bucketName);

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

  ;

  private final Transcoder<StringKeyValueDocument<V>, V> defaultTranscoder = new Transcoder<StringKeyValueDocument<V>, V>() {
    @Override
    public StringKeyValueDocument<V> decode(String id, ByteBuf content, long cas, int expiry, int flags,
        ResponseStatus status) {
      return null;
    }

    @Override
    public Tuple2<ByteBuf, Integer> encode(StringKeyValueDocument<V> document) {
      return null;
    }

    @Override
    public StringKeyValueDocument<V> newDocument(String id, int expiry, V content, long cas) {
      return null;
    }

    @Override
    public StringKeyValueDocument<V> newDocument(String id, int expiry, V content, long cas,
        MutationToken mutationToken) {
      return null;
    }

    @Override
    public Class<StringKeyValueDocument<V>> documentType() {
      return StringKeyValueDocument.class<V>;
    }
  }

  @Override
  public void writeImpl(D record)
      throws IOException {

    try {
      _bucket.upsert(new StringKeyValueDocument<V>(record.getKey(), record.getValue()));
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
