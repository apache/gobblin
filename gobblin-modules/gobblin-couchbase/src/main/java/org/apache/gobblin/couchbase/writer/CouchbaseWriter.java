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

import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.auth.CertAuthenticator;
import com.couchbase.client.java.document.AbstractDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.transcoder.Transcoder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.util.Pair;
import org.apache.gobblin.couchbase.common.TupleDocument;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.AsyncDataWriter;
import org.apache.gobblin.writer.GenericWriteResponse;
import org.apache.gobblin.writer.GenericWriteResponseWrapper;
import org.apache.gobblin.writer.SyncDataWriter;
import org.apache.gobblin.writer.WriteCallback;
import org.apache.gobblin.writer.WriteResponse;
import org.apache.gobblin.writer.WriteResponseFuture;
import org.apache.gobblin.writer.WriteResponseMapper;
import rx.Observable;
import rx.Subscriber;


/**
 * A single bucket Couchbase writer.
 */
@Slf4j
public class CouchbaseWriter<D extends AbstractDocument> implements AsyncDataWriter<D>, SyncDataWriter<D> {

  private final Cluster _cluster;
  private final Bucket _bucket;
  private final long _operationTimeout;
  private final int _documentTTL;
  private final TimeUnit _documentTTLTimeUnits;
  private final String _documentTTLOriginField;
  private final TimeUnit _documentTTLOriginUnits;

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
    boolean usesCertAuth = ConfigUtils.getBoolean(config, CouchbaseWriterConfigurationKeys.CERT_AUTH_ENABLED, false);
    String password = ConfigUtils.getString(config, CouchbaseWriterConfigurationKeys.PASSWORD, "");

    log.info("Using hosts hosts: {}", hosts.stream().collect(Collectors.joining(",")));

    _documentTTL = ConfigUtils.getInt(config, CouchbaseWriterConfigurationKeys.DOCUMENT_TTL, 0);
    _documentTTLTimeUnits =
        ConfigUtils.getTimeUnit(config, CouchbaseWriterConfigurationKeys.DOCUMENT_TTL_UNIT, CouchbaseWriterConfigurationKeys.DOCUMENT_TTL_UNIT_DEFAULT);
    _documentTTLOriginField =
        ConfigUtils.getString(config, CouchbaseWriterConfigurationKeys.DOCUMENT_TTL_ORIGIN_FIELD, null);
    _documentTTLOriginUnits =
        ConfigUtils.getTimeUnit(config, CouchbaseWriterConfigurationKeys.DOCUMENT_TTL_ORIGIN_FIELD_UNITS,
            CouchbaseWriterConfigurationKeys.DOCUMENT_TTL_ORIGIN_FIELD_UNITS_DEFAULT);

    String bucketName = ConfigUtils.getString(config, CouchbaseWriterConfigurationKeys.BUCKET,
        CouchbaseWriterConfigurationKeys.BUCKET_DEFAULT);

    _cluster = CouchbaseCluster.create(couchbaseEnvironment, hosts);

    if (usesCertAuth) {
      _cluster.authenticate(CertAuthenticator.INSTANCE);
      _bucket = _cluster.openBucket(bucketName, Collections.singletonList(_tupleDocumentTranscoder));
    } else if (password.isEmpty()) {
      _bucket = _cluster.openBucket(bucketName, Collections.singletonList(_tupleDocumentTranscoder));
    } else {
      _bucket = _cluster.openBucket(bucketName, password, Collections.singletonList(_tupleDocumentTranscoder));
    }
    _operationTimeout = ConfigUtils.getLong(config, CouchbaseWriterConfigurationKeys.OPERATION_TIMEOUT_MILLIS,
        CouchbaseWriterConfigurationKeys.OPERATION_TIMEOUT_DEFAULT);
    _operationTimeunit = TimeUnit.MILLISECONDS;

    _defaultWriteResponseMapper = new GenericWriteResponseWrapper<>();

    log.info("Couchbase writer configured with: hosts: {}, bucketName: {}, operationTimeoutInMillis: {}", hosts,
        bucketName, _operationTimeout);
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
    Observable<D> observable;
    try {
      observable = _bucket.async().upsert(setDocumentTTL(record));
    } catch (DataRecordException e) {
      throw new RuntimeException("Caught exception trying to set TTL of the document", e);
    }

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
        public WriteResponse get() throws InterruptedException, ExecutionException {
          Pair<WriteResponse, Throwable> writeResponseThrowablePair = writeResponseQueue.take();
          return getWriteResponseOrThrow(writeResponseThrowablePair);
        }

        @Override
        public WriteResponse get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
          Pair<WriteResponse, Throwable> writeResponseThrowablePair = writeResponseQueue.poll(timeout, unit);
          if (writeResponseThrowablePair == null) {
            throw new TimeoutException("Timeout exceeded while waiting for future to be done");
          } else {
            return getWriteResponseOrThrow(writeResponseThrowablePair);
          }
        }
      };

      observable.timeout(_operationTimeout, _operationTimeunit).subscribe(new Subscriber<D>() {
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
  public void flush() throws IOException {

  }

  private WriteResponse getWriteResponseOrThrow(Pair<WriteResponse, Throwable> writeResponseThrowablePair)
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
  public void cleanup() throws IOException {

  }

  /**
   * Returns a new document with 32 bit (int) timestamp expiration date for the document. Note this is a current limitation in couchbase.
   * This approach should work for documents that do not expire until 2038. This should be enough headroom for couchbase
   * to reimplement the design.
   * Source: https://forums.couchbase.com/t/document-expiry-in-seconds-or-a-timestamp/6519/6
   * @param record
   * @return
   */
  private D setDocumentTTL(D record) throws DataRecordException {
    boolean recordIsTupleDocument = record instanceof TupleDocument;
    boolean recordIsJsonDocument = record instanceof RawJsonDocument;

    long ttlSpanSec = TimeUnit.SECONDS.convert(_documentTTL, _documentTTLTimeUnits);
    long eventOriginSec = 0;
    String dataJson = null;
    if (_documentTTL == 0) {
      return record;
    } else if (_documentTTLOriginField != null && !_documentTTLOriginField.isEmpty()) {

      if (recordIsTupleDocument) {
        ByteBuf dataByteBuffer = ((Tuple2<ByteBuf, Integer>) record.content()).value1();
        dataJson = new String(dataByteBuffer.array(), StandardCharsets.UTF_8);
      } else {
        dataJson = (String) record.content();
      }
      JsonElement jsonDataRootElement = new JsonParser().parse(dataJson);
      if (!jsonDataRootElement.isJsonObject()) {
        throw new DataRecordException(
            String.format("Document TTL Field is set but the record's value is not a valid json object.: '%s'",
                jsonDataRootElement.toString()));
      }
      JsonObject jsonDataRoot = jsonDataRootElement.getAsJsonObject();
      long documentTTLOrigin = jsonDataRoot.get(_documentTTLOriginField).getAsLong();
      eventOriginSec = TimeUnit.SECONDS.convert(documentTTLOrigin, _documentTTLOriginUnits);
    } else {
      eventOriginSec = System.currentTimeMillis() / 1000;
    }
    try {
      int expiration = Math.toIntExact(ttlSpanSec + eventOriginSec);
      if (recordIsTupleDocument) {
        return (D) _tupleDocumentTranscoder.newDocument(record.id(), expiration,
            (Tuple2<ByteBuf, Integer>) record.content(), record.cas(), record.mutationToken());
      } else if (recordIsJsonDocument) {
        return (D) RawJsonDocument.create(record.id(), expiration, (String) record.content(), record.cas(),
            record.mutationToken());
      } else {
        throw new RuntimeException(" Only TupleDocument and RawJsonDocument documents are supported");
      }
    } catch (ArithmeticException e) {
      throw new RuntimeException(
          "There was an overflow calculating the expiry timestamp. couchbase currently only supports expiry until January 19, 2038 03:14:07 GMT",
          e);
    }
  }

  @Override
  public WriteResponse write(D record) throws IOException {
    try {
      D doc = _bucket.upsert(setDocumentTTL(record));

      Preconditions.checkNotNull(doc);
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
