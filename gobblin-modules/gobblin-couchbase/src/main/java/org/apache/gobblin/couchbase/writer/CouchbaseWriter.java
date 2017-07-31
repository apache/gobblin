/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.couchbase.writer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.math3.util.Pair;

import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.AbstractDocument;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.transcoder.Transcoder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.Subscriber;

import org.apache.gobblin.couchbase.common.TupleDocument;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.AsyncDataWriter;
import org.apache.gobblin.writer.GenericWriteResponse;
import org.apache.gobblin.writer.GenericWriteResponseWrapper;
import org.apache.gobblin.writer.SyncDataWriter;
import org.apache.gobblin.writer.WriteCallback;
import org.apache.gobblin.writer.WriteResponse;
import org.apache.gobblin.writer.WriteResponseFuture;
import org.apache.gobblin.writer.WriteResponseMapper;


/**
 * A single bucket Couchbase writer.
 */
@Slf4j
public class CouchbaseWriter<D extends AbstractDocument> implements AsyncDataWriter<D>, SyncDataWriter<D> {

  private final Cluster _cluster;
  private final Bucket _bucket;
  private final long _operationTimeout;
  private final TimeUnit _operationTimeunit;
  private final WriteResponseMapper<D> _defaultWriteResponseMapper;

  // A basic transcoder that just passes through the embedded binary content.
  private final Transcoder<TupleDocument, Tuple2<ByteBuf, Integer>> _tupleDocumentTranscoder =
      new Transcoder<TupleDocument, Tuple2<ByteBuf, Integer>>() {
        @Override
        public TupleDocument decode(String id, ByteBuf content, long cas, int expiry, int flags,
            ResponseStatus status) {
          return newDocument(id, expiry, Tuple.create(content, flags), cas);
        }

        @Override
        public Tuple2<ByteBuf, Integer> encode(TupleDocument document) {
          return document.content();
        }

        @Override
        public TupleDocument newDocument(String id, int expiry, Tuple2<ByteBuf, Integer> content, long cas) {
          return new TupleDocument(id, expiry, content, cas);
        }

        @Override
        public TupleDocument newDocument(String id, int expiry, Tuple2<ByteBuf, Integer> content, long cas,
            MutationToken mutationToken) {
          return new TupleDocument(id, expiry, content, cas);
        }

        @Override
        public Class<TupleDocument> documentType() {
          return TupleDocument.class;
        }
      };

  public CouchbaseWriter(CouchbaseEnvironment couchbaseEnvironment, Config config) {

    List<String> hosts = ConfigUtils.getStringList(config, CouchbaseWriterConfigurationKeys.BOOTSTRAP_SERVERS);

    _cluster = CouchbaseCluster.create(couchbaseEnvironment, hosts);

    String bucketName = ConfigUtils.getString(config, CouchbaseWriterConfigurationKeys.BUCKET,
        CouchbaseWriterConfigurationKeys.BUCKET_DEFAULT);

    String password = ConfigUtils.getString(config, CouchbaseWriterConfigurationKeys.PASSWORD, "");

    _bucket = _cluster.openBucket(bucketName, password,
        Collections.<Transcoder<? extends Document, ?>>singletonList(_tupleDocumentTranscoder));

    _operationTimeout = ConfigUtils.getLong(config, CouchbaseWriterConfigurationKeys.OPERATION_TIMEOUT_MILLIS,
        CouchbaseWriterConfigurationKeys.OPERATION_TIMEOUT_DEFAULT);
    _operationTimeunit = TimeUnit.MILLISECONDS;

    _defaultWriteResponseMapper = new GenericWriteResponseWrapper<>();

    log.info("Couchbase writer configured with: hosts: {}, bucketName: {}, operationTimeoutInMillis: {}",
        hosts, bucketName, _operationTimeout);
  }

  @VisibleForTesting
  Bucket getBucket() {
    return _bucket;
  }

  private void assertRecordWritable(D record) {
    boolean recordIsTupleDocument = (record instanceof TupleDocument);
    boolean recordIsJsonDocument = (record instanceof RawJsonDocument);
    Preconditions.checkArgument(recordIsTupleDocument || recordIsJsonDocument,
        "This writer only supports TupleDocument or RawJsonDocument. Found " + record.getClass().getName());
  }

  @Override
  public Future<WriteResponse> write(final D record, final WriteCallback callback) {
    assertRecordWritable(record);
    if (record instanceof TupleDocument) {
      ((TupleDocument) record).content().value1().retain();
    }
    Observable<D> observable = _bucket.async().upsert(record);
    if (callback == null) {
      return new WriteResponseFuture<>(
          observable.timeout(_operationTimeout, _operationTimeunit).toBlocking().toFuture(),
          _defaultWriteResponseMapper);
    } else {

      final AtomicBoolean callbackFired = new AtomicBoolean(false);
      final BlockingQueue<Pair<WriteResponse, Throwable>> writeResponseQueue = new ArrayBlockingQueue<>(1);

      final Future<WriteResponse> writeResponseFuture = new Future<WriteResponse>() {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
          return false;
        }

        @Override
        public boolean isCancelled() {
          return false;
        }

        @Override
        public boolean isDone() {
          return callbackFired.get();
        }

        @Override
        public WriteResponse get()
            throws InterruptedException, ExecutionException {
          Pair<WriteResponse, Throwable> writeResponseThrowablePair = writeResponseQueue.take();
          return getWriteResponseorThrow(writeResponseThrowablePair);
        }

        @Override
        public WriteResponse get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
          Pair<WriteResponse, Throwable> writeResponseThrowablePair = writeResponseQueue.poll(timeout, unit);
          if (writeResponseThrowablePair == null) {
            throw new TimeoutException("Timeout exceeded while waiting for future to be done");
          } else {
            return getWriteResponseorThrow(writeResponseThrowablePair);
          }
        }
      };

      observable.timeout(_operationTimeout, _operationTimeunit)
          .subscribe(new Subscriber<D>() {
            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable e) {
              callbackFired.set(true);
              writeResponseQueue.add(new Pair<WriteResponse, Throwable>(null, e));
              callback.onFailure(e);
            }

            @Override
            public void onNext(D doc) {
              try {
                callbackFired.set(true);
                WriteResponse writeResponse = new GenericWriteResponse<D>(doc);
                writeResponseQueue.add(new Pair<WriteResponse, Throwable>(writeResponse, null));
                callback.onSuccess(writeResponse);
              } finally {
                if (doc instanceof TupleDocument) {
                  ((TupleDocument) doc).content().value1().release();
                }
              }
            }
          });
      return writeResponseFuture;
    }
  }

  @Override
  public void flush()
      throws IOException {

  }

  private WriteResponse getWriteResponseorThrow(Pair<WriteResponse, Throwable> writeResponseThrowablePair)
      throws ExecutionException {
    if (writeResponseThrowablePair.getFirst() != null) {
      return writeResponseThrowablePair.getFirst();
    } else if (writeResponseThrowablePair.getSecond() != null) {
      throw new ExecutionException(writeResponseThrowablePair.getSecond());
    } else {
      throw new ExecutionException(new RuntimeException("Could not find non-null WriteResponse pair"));
    }
  }

  @Override
  public void cleanup()
      throws IOException {

  }

  @Override
  public WriteResponse write(D record)
      throws IOException {

    try {
      D doc = _bucket.upsert(record);
      return new GenericWriteResponse(doc);
    } catch (Exception e) {
      throw new IOException("Failed to write to Couchbase cluster", e);
    }
  }

  @Override
  public void close() {
    if (!_bucket.isClosed()) {
      try {
        _bucket.close();
      } catch (Exception e) {
        log.warn("Failed to close bucket", e);
      }
    }
    try {
      _cluster.disconnect();
    } catch (Exception e) {
      log.warn("Failed to disconnect from cluster", e);
    }
  }
}
