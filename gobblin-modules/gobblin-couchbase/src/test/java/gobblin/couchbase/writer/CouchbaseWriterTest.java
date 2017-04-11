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

package gobblin.couchbase.writer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.util.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.AbstractDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.google.gson.Gson;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.couchbase.CouchbaseTestServer;
import gobblin.couchbase.common.TupleDocument;
import gobblin.couchbase.converter.AnyToCouchbaseJsonConverter;
import gobblin.couchbase.converter.AvroToCouchbaseTupleConverter;
import gobblin.metrics.RootMetricContext;
import gobblin.metrics.reporter.OutputStreamReporter;
import gobblin.test.TestUtils;
import gobblin.writer.AsyncWriterManager;
import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;

@Slf4j
public class CouchbaseWriterTest {

  private CouchbaseTestServer _couchbaseTestServer;
  private CouchbaseEnvironment _couchbaseEnvironment;

  @BeforeSuite
  public void startServers() {
    _couchbaseTestServer = new CouchbaseTestServer(TestUtils.findFreePort());
    _couchbaseTestServer.start();
    _couchbaseEnvironment = DefaultCouchbaseEnvironment.builder().bootstrapHttpEnabled(true)
        .bootstrapHttpDirectPort(_couchbaseTestServer.getPort())
        .bootstrapCarrierDirectPort(_couchbaseTestServer.getServerPort()).bootstrapCarrierEnabled(false)
        .kvTimeout(10000).build();
  }


  /**
   * Implement the equivalent of:
   * curl -XPOST -u Administrator:password localhost:httpPort/pools/default/buckets \ -d bucketType=couchbase \
   * -d name={@param bucketName} -d authType=sasl -d ramQuotaMB=200
   **/
  private boolean createBucket(String bucketName) {
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    try {
      HttpPost httpPost = new HttpPost("http://localhost:" + _couchbaseTestServer.getPort() + "/pools/default/buckets");
      List<NameValuePair> params = new ArrayList<>(2);
      params.add(new BasicNameValuePair("bucketType", "couchbase"));
      params.add(new BasicNameValuePair("name", bucketName));
      params.add(new BasicNameValuePair("authType", "sasl"));
      params.add(new BasicNameValuePair("ramQuotaMB", "200"));
      httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
      //Execute and get the response.
      HttpResponse response = httpClient.execute(httpPost);
      log.info(String.valueOf(response.getStatusLine().getStatusCode()));
      return true;
    }
    catch (Exception e) {
      log.error("Failed to create bucket {}", bucketName, e);
      return false;
    }
  }

  @AfterSuite
  public void stopServers() {
    _couchbaseTestServer.stop();
  }

  /**
   * Test that a single tuple document can be written successfully.
   * @throws IOException
   * @throws DataConversionException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test
  public void testTupleDocumentWrite()
      throws IOException, DataConversionException, ExecutionException, InterruptedException {
    Properties props = new Properties();
    props.setProperty(CouchbaseWriterConfigurationKeys.BUCKET, "default");
    Config config = ConfigFactory.parseProperties(props);

    CouchbaseWriter writer = new CouchbaseWriter(_couchbaseEnvironment, config);
    try {
      Schema dataRecordSchema =
          SchemaBuilder.record("Data").fields().name("data").type().bytesType().noDefault().name("flags").type().intType()
              .noDefault().endRecord();

      Schema schema = SchemaBuilder.record("TestRecord").fields().name("key").type().stringType().noDefault().name("data")
          .type(dataRecordSchema).noDefault().endRecord();

      GenericData.Record testRecord = new GenericData.Record(schema);

      String testContent = "hello world";

      GenericData.Record dataRecord = new GenericData.Record(dataRecordSchema);
      dataRecord.put("data", ByteBuffer.wrap(testContent.getBytes(Charset.forName("UTF-8"))));
      dataRecord.put("flags", 0);

      testRecord.put("key", "hello");
      testRecord.put("data", dataRecord);

      Converter<Schema, String, GenericRecord, TupleDocument> recordConverter = new AvroToCouchbaseTupleConverter();

      TupleDocument doc = recordConverter.convertRecord("", testRecord, null).iterator().next();
      writer.write(doc, null).get();
      TupleDocument returnDoc = writer.getBucket().get("hello", TupleDocument.class);

      byte[] returnedBytes = new byte[returnDoc.content().value1().readableBytes()];
      returnDoc.content().value1().readBytes(returnedBytes);
      Assert.assertEquals(returnedBytes, testContent.getBytes(Charset.forName("UTF-8")));

      int returnedFlags = returnDoc.content().value2();
      Assert.assertEquals(returnedFlags, 0);

    } finally {
      writer.close();
    }

  }

  /**
   * Test that a single Json document can be written successfully
   * @throws IOException
   * @throws DataConversionException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test(groups={"timeout"})
  public void testJsonDocumentWrite()
      throws IOException, DataConversionException, ExecutionException, InterruptedException {
    CouchbaseWriter writer = new CouchbaseWriter(_couchbaseEnvironment, ConfigFactory.empty());
    try {

      String key = "hello";
      String testContent = "hello world";
      HashMap<String, String> contentMap = new HashMap<>();
      contentMap.put("value", testContent);
      Gson gson = new Gson();
      String jsonString = gson.toJson(contentMap);
      RawJsonDocument jsonDocument = RawJsonDocument.create(key, jsonString);
      writer.write(jsonDocument, null).get();
      RawJsonDocument returnDoc = writer.getBucket().get(key, RawJsonDocument.class);

      Map<String, String> returnedMap = gson.fromJson(returnDoc.content(), Map.class);
      Assert.assertEquals(testContent, returnedMap.get("value"));
    } finally {
      writer.close();
    }
  }

  private void drainQueue(BlockingQueue<Pair<AbstractDocument, Future>> queue, int threshold, long sleepTime,
      TimeUnit sleepUnit, List<Pair<AbstractDocument, Future>> failedFutures) {
    while (queue.remainingCapacity() < threshold) {
      if (sleepTime > 0) {
        Pair<AbstractDocument, Future> topElement = queue.peek();
        if (topElement != null) {
          try {
            topElement.getSecond().get(sleepTime, sleepUnit);
          } catch (Exception te) {
            failedFutures.add(topElement);
          }
          queue.poll();
        }
      }
    }
  }

  /**
   * An iterator that applies the {@link AnyToCouchbaseJsonConverter} converter to Objects
   */
  class JsonDocumentIterator implements Iterator<AbstractDocument> {
    private final int _maxRecords;
    private int _currRecord;
    private Iterator<Object> _objectIterator;
    private final Converter<String, String, Object, RawJsonDocument> _recordConverter =
        new AnyToCouchbaseJsonConverter();

    JsonDocumentIterator(Iterator<Object> genericRecordIterator) {
      this(genericRecordIterator, -1);
    }

    JsonDocumentIterator(Iterator<Object> genericRecordIterator, int maxRecords) {
      _objectIterator = genericRecordIterator;
      _maxRecords = maxRecords;
      _currRecord = 0;
    }

    @Override
    public boolean hasNext() {
      if (_maxRecords < 0) {
        return _objectIterator.hasNext();
      } else {
        return _objectIterator.hasNext() && (_currRecord < _maxRecords);
      }
    }

    @Override
    public AbstractDocument next() {
      _currRecord++;
      Object record = _objectIterator.next();
      try {
        return _recordConverter.convertRecord("", record, null).iterator().next();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void remove() {
    }

  }

  /**
   * An iterator that applies the {@link AvroToCouchbaseTupleConverter} converter to GenericRecords
   */
  class TupleDocumentIterator implements Iterator<AbstractDocument> {
    private final int _maxRecords;
    private int _currRecord;
    private Iterator<GenericRecord> _genericRecordIterator;
    private final Converter<Schema, String, GenericRecord, TupleDocument> _recordConverter =
        new AvroToCouchbaseTupleConverter();

    TupleDocumentIterator(Iterator<GenericRecord> genericRecordIterator) {
      this(genericRecordIterator, -1);
    }

    TupleDocumentIterator(Iterator<GenericRecord> genericRecordIterator, int maxRecords) {
      _genericRecordIterator = genericRecordIterator;
      _maxRecords = maxRecords;
      _currRecord = 0;
    }

    @Override
    public boolean hasNext() {
      if (_maxRecords < 0) {
        return _genericRecordIterator.hasNext();
      } else {
        return _genericRecordIterator.hasNext() && (_currRecord < _maxRecords);
      }
    }

    @Override
    public TupleDocument next() {
      _currRecord++;
      GenericRecord record = _genericRecordIterator.next();
      try {
        return _recordConverter.convertRecord("", record, null).iterator().next();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void remove() {
    }

  }

  class Verifier {
    private final Map<String, byte[]> verificationCache = new HashMap<>(1000);
    private Class recordClass;

    void onWrite(AbstractDocument doc)
        throws UnsupportedEncodingException {
      recordClass = doc.getClass();
      if (doc instanceof TupleDocument) {
        ByteBuf outgoingBuf = (((TupleDocument) doc).content()).value1();
        byte[] outgoingBytes = new byte[outgoingBuf.readableBytes()];
        outgoingBuf.getBytes(0, outgoingBytes);
        verificationCache.put(doc.id(), outgoingBytes);
      } else if (doc instanceof RawJsonDocument) {
        verificationCache.put(doc.id(), ((RawJsonDocument) doc).content().getBytes("UTF-8"));
      } else {
        throw new UnsupportedOperationException("Can only support TupleDocument or RawJsonDocument at this time");
      }
    }

    void verify(Bucket bucket)
        throws UnsupportedEncodingException {
      // verify
      System.out.println("Starting verification procedure");
      for (Map.Entry<String, byte[]> cacheEntry : verificationCache.entrySet()) {
        Object doc = bucket.get(cacheEntry.getKey(), recordClass);
        if (doc instanceof TupleDocument) {
          ByteBuf returnedBuf = (((TupleDocument) doc).content()).value1();
          byte[] returnedBytes = new byte[returnedBuf.readableBytes()];
          returnedBuf.getBytes(0, returnedBytes);
          Assert.assertEquals(returnedBytes, cacheEntry.getValue(), "Returned content for TupleDoc should be equal");
        } else if (doc instanceof RawJsonDocument) {
          byte[] returnedBytes = ((RawJsonDocument) doc).content().getBytes("UTF-8");
          Assert.assertEquals(returnedBytes, cacheEntry.getValue(), "Returned content for JsonDoc should be equal");
        } else {
          Assert.fail("Returned type was neither TupleDocument nor RawJsonDocument");
        }
      }
      System.out.println("Verification success!");
    }
  }

  /**
   * Uses the {@link AsyncWriterManager} to write records through a couchbase writer
   * It keeps a copy of the key, value combinations written and checks after all the writes have gone through.
   * @param recordIterator
   * @throws IOException
   */
  private void writeRecordsWithAsyncWriter(Iterator<AbstractDocument> recordIterator)
      throws IOException {
    boolean verbose = false;
    Properties props = new Properties();
    props.setProperty(CouchbaseWriterConfigurationKeys.BUCKET, "default");
    Config config = ConfigFactory.parseProperties(props);

    CouchbaseWriter writer = new CouchbaseWriter(_couchbaseEnvironment, config);

    try {
      AsyncWriterManager asyncWriterManager =
          AsyncWriterManager.builder().asyncDataWriter(writer).maxOutstandingWrites(100000).retriesEnabled(true)
              .numRetries(5).build();

      if (verbose) {
        // Create a reporter for metrics. This reporter will write metrics to STDOUT.
        OutputStreamReporter.Factory.newBuilder().build(new Properties());
        // Start all metric reporters.
        RootMetricContext.get().startReporting();
      }

      Verifier verifier = new Verifier();
      while (recordIterator.hasNext()) {
        AbstractDocument doc = recordIterator.next();
        verifier.onWrite(doc);
        asyncWriterManager.write(doc);
      }
      asyncWriterManager.commit();
      verifier.verify(writer.getBucket());
    } finally {
      writer.close();
    }
  }

  private List<Pair<AbstractDocument, Future>> writeRecords(Iterator<AbstractDocument> recordIterator,
      CouchbaseWriter writer, int outstandingRequests, long kvTimeout, TimeUnit kvTimeoutUnit)
      throws DataConversionException, UnsupportedEncodingException {
    final BlockingQueue<Pair<AbstractDocument, Future>> outstandingCallQueue =
        new LinkedBlockingDeque<>(outstandingRequests);
    final List<Pair<AbstractDocument, Future>> failedFutures = new ArrayList<>(outstandingRequests);

    int index = 0;
    long runTime = 0;
    final AtomicInteger callbackSuccesses = new AtomicInteger(0);
    final AtomicInteger callbackFailures = new AtomicInteger(0);
    final ConcurrentLinkedDeque<Throwable> callbackExceptions = new ConcurrentLinkedDeque<>();
    Verifier verifier = new Verifier();
    while (recordIterator.hasNext()) {
      AbstractDocument doc = recordIterator.next();
      index++;
      verifier.onWrite(doc);
      final long startTime = System.nanoTime();
      Future callFuture = writer.write(doc, new WriteCallback<TupleDocument>() {
        @Override
        public void onSuccess(WriteResponse<TupleDocument> writeResponse) {
          callbackSuccesses.incrementAndGet();
        }

        @Override
        public void onFailure(Throwable throwable) {
          callbackFailures.incrementAndGet();
          callbackExceptions.add(throwable);
        }
      });
      drainQueue(outstandingCallQueue, 1, kvTimeout, kvTimeoutUnit, failedFutures);
      outstandingCallQueue.add(new Pair<>(doc, callFuture));
      runTime += System.nanoTime() - startTime;
    }
    int failedWrites = 0;
    long responseStartTime = System.nanoTime();
    drainQueue(outstandingCallQueue, outstandingRequests, kvTimeout, kvTimeoutUnit, failedFutures);
    runTime += System.nanoTime() - responseStartTime;

    for (Throwable failure : callbackExceptions) {
      System.out.println(failure.getClass() + " : " + failure.getMessage());
    }
    failedWrites += failedFutures.size();

    System.out.println(
        "Total time to send " + index + " records = " + runTime / 1000000.0 + "ms, " + "Failed writes = " + failedWrites
            + " Callback Successes = " + callbackSuccesses.get() + "Callback Failures = " + callbackFailures.get());

    verifier.verify(writer.getBucket());
    return failedFutures;
  }

  @Test
  public void testMultiTupleDocumentWrite()
      throws IOException, DataConversionException, ExecutionException, InterruptedException {
    CouchbaseWriter writer = new CouchbaseWriter(_couchbaseEnvironment, ConfigFactory.empty());
    try {

      final Schema dataRecordSchema =
          SchemaBuilder.record("Data").fields().name("data").type().bytesType().noDefault().name("flags").type().intType()
              .noDefault().endRecord();

      final Schema schema =
          SchemaBuilder.record("TestRecord").fields().name("key").type().stringType().noDefault().name("data")
              .type(dataRecordSchema).noDefault().endRecord();

      final int numRecords = 1000;
      int outstandingRequests = 99;

      Iterator<GenericRecord> recordIterator = new Iterator<GenericRecord>() {
        private int currentIndex;

        @Override
        public void remove() {
        }

        @Override
        public boolean hasNext() {
          return (currentIndex < numRecords);
        }

        @Override
        public GenericRecord next() {
          GenericData.Record testRecord = new GenericData.Record(schema);

          String testContent = "hello world" + currentIndex;
          GenericData.Record dataRecord = new GenericData.Record(dataRecordSchema);
          dataRecord.put("data", ByteBuffer.wrap(testContent.getBytes(Charset.forName("UTF-8"))));
          dataRecord.put("flags", 0);

          testRecord.put("key", "hello" + currentIndex);
          testRecord.put("data", dataRecord);
          currentIndex++;
          return testRecord;
        }
      };

      long kvTimeout = 10000;
      TimeUnit kvTimeoutUnit = TimeUnit.MILLISECONDS;
      writeRecords(new TupleDocumentIterator(recordIterator), writer, outstandingRequests, kvTimeout, kvTimeoutUnit);
    } finally {
      writer.close();
    }
  }

  @Test
  public void testMultiJsonDocumentWriteWithAsyncWriter()
      throws IOException, DataConversionException, ExecutionException, InterruptedException {

    final int numRecords = 1000;

    Iterator<Object> recordIterator = new Iterator<Object>() {
      private int currentIndex;

      @Override
      public boolean hasNext() {
        return (currentIndex < numRecords);
      }

      @Override
      public Object next() {

        String testContent = "hello world" + currentIndex;
        String key = "hello" + currentIndex;
        HashMap<String, String> contentMap = new HashMap<>();
        contentMap.put("key", key);
        contentMap.put("value", testContent);
        currentIndex++;
        return contentMap;
      }

      @Override
      public void remove() {
      }

    };
    writeRecordsWithAsyncWriter(new JsonDocumentIterator(recordIterator));
  }

  @Test
  public void testMultiTupleDocumentWriteWithAsyncWriter()
      throws IOException, DataConversionException, ExecutionException, InterruptedException {
    final Schema dataRecordSchema =
        SchemaBuilder.record("Data").fields().name("data").type().bytesType().noDefault().name("flags").type().intType()
            .noDefault().endRecord();

    final Schema schema =
        SchemaBuilder.record("TestRecord").fields().name("key").type().stringType().noDefault().name("data")
            .type(dataRecordSchema).noDefault().endRecord();

    final int numRecords = 1000;

    Iterator<GenericRecord> recordIterator = new Iterator<GenericRecord>() {
      private int currentIndex;

      @Override
      public void remove() {
      }

      @Override
      public boolean hasNext() {
        return (currentIndex < numRecords);
      }

      @Override
      public GenericRecord next() {
        GenericData.Record testRecord = new GenericData.Record(schema);

        String testContent = "hello world" + currentIndex;
        GenericData.Record dataRecord = new GenericData.Record(dataRecordSchema);
        dataRecord.put("data", ByteBuffer.wrap(testContent.getBytes(Charset.forName("UTF-8"))));
        dataRecord.put("flags", 0);

        testRecord.put("key", "hello" + currentIndex);
        testRecord.put("data", dataRecord);
        currentIndex++;
        return testRecord;
      }
    };
    writeRecordsWithAsyncWriter(new TupleDocumentIterator(recordIterator));
  }
}
