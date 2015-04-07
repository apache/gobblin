package gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;


/**
 * A schema registry class that provides two services: get the latest schema of a topic, and register a schema.
 *
 * @author ziliu
 *
 */
public class KafkaAvroSchemaRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroSchemaRegistry.class);

  private static final int GET_SCHEMA_BY_ID_MAX_TIRES = 3;
  private static final int GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS = 1;
  private static final String GET_RESOURCE_BY_ID = "/id=";
  private static final String GET_RESOURCE_BY_TYPE = "/latest_with_type=";
  private static final String SCHEMA_ID_HEADER_NAME = "Location";
  private static final String SCHEMA_ID_HEADER_PREFIX = "/id=";

  private final LoadingCache<String, Schema> cachedSchemasById;

  private final HttpClient httpClient;

  private final String url;

  public KafkaAvroSchemaRegistry(String url) {
    this.url = url;
    this.cachedSchemasById =
        CacheBuilder.newBuilder().maximumSize(1000).expireAfterWrite(10, TimeUnit.MINUTES)
            .build(new KafkaSchemaCacheLoader());
    this.httpClient = new HttpClient(new MultiThreadedHttpConnectionManager());
  }

  /**
   * Get schema from schema registry by ID
   *
   * @param id Schema ID
   * @return Schema with the corresponding ID
   * @throws SchemaNotFoundException if ID not found
   */
  public synchronized Schema getSchemaById(String id) throws SchemaNotFoundException {
    try {
      return cachedSchemasById.get(id);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the latest schema of a topic.
   *
   * @param topic topic name
   * @return the latest schema
   * @throws SchemaNotFoundException if topic name not found.
   */
  public synchronized Schema getLatestSchemaByTopic(String topic) throws SchemaNotFoundException {
    String schemaUrl = KafkaAvroSchemaRegistry.this.url + GET_RESOURCE_BY_TYPE + topic;

    LOG.debug("Fetching from URL : " + schemaUrl);

    GetMethod get = new GetMethod(schemaUrl);

    int statusCode;
    String schemaString;
    try {
      statusCode = KafkaAvroSchemaRegistry.this.httpClient.executeMethod(get);
      schemaString = get.getResponseBodyAsString();
    } catch (HttpException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      get.releaseConnection();
    }

    if (statusCode != HttpStatus.SC_OK) {
      throw new SchemaNotFoundException(String.format("Latest schema for topic %s cannot be retrieved", topic));
    }

    Schema schema;
    if (schemaString.startsWith("{")) {
      try {
        schema = new Schema.Parser().parse(schemaString);
      } catch (Exception e) {
        throw new SchemaNotFoundException(String.format("Latest schema for topic %s cannot be retrieved", topic));
      }
    } else {
      throw new SchemaNotFoundException(String.format("Latest schema for topic %s cannot be retrieved", topic));
    }

    return schema;
  }

  /**
   * Register a schema to the Kafka schema registry
   *
   * @param schema
   * @return schema ID of the registered schema
   */
  public synchronized String register(Schema schema) {
    LOG.info("Registering schema " + schema.toString());

    PostMethod post = new PostMethod(url);
    post.addParameter("schema", schema.toString());

    int statusCode;
    try {
      LOG.debug("Loading: " + post.getURI());
      statusCode = httpClient.executeMethod(post);
      if (statusCode != HttpStatus.SC_CREATED)
        LOG.error("Error occurred while trying to register schema: " + statusCode);
    } catch (URIException e) {
      throw new RuntimeException(e);
    } catch (HttpException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      post.releaseConnection();
    }

    LOG.info("Registered schema successfully");
    String response;
    try {
      response = post.getResponseBodyAsString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    String schemaId;
    Header[] headers = post.getResponseHeaders(SCHEMA_ID_HEADER_NAME);
    if (headers.length != 1) {
      LOG.error("Error reading schemaId returned by registerSchema call");
      throw new RuntimeException("Error reading schemaId returned by registerSchema call");
    } else if (!headers[0].getValue().startsWith(SCHEMA_ID_HEADER_PREFIX)) {
      LOG.error("Error parsing schemaId returned by registerSchema call");
      throw new RuntimeException("Error parsing schemaId returned by registerSchema call");
    } else {
      schemaId = headers[0].getValue().substring(SCHEMA_ID_HEADER_PREFIX.length());
    }

    if (response != null) {
      LOG.info("Received response " + response);
    }

    return schemaId;
  }

  private class KafkaSchemaCacheLoader extends CacheLoader<String, Schema> {

    private final Map<String, FailedFetchHistory> failedFetchHistories;

    private KafkaSchemaCacheLoader() {
      super();
      this.failedFetchHistories = Maps.newHashMap();
    }

    @Override
    public Schema load(String id) throws Exception {
      if (shouldFetchFromSchemaRegistry(id)) {
        try {
          return fetchSchemaByID(id);
        } catch (SchemaNotFoundException e) {
          addFetchToFailureHistory(id);
          throw e;
        }
      }
      throw new SchemaNotFoundException(String.format("Schema with ID = %s cannot be retrieved", id));
    }

    private void addFetchToFailureHistory(String id) {
      if (!failedFetchHistories.containsKey(id)) {
        failedFetchHistories.put(id, new FailedFetchHistory(1, System.nanoTime()));
      } else {
        failedFetchHistories.get(id).incrementNumOfAttempts();
        failedFetchHistories.get(id).setPreviousAttemptTime(System.nanoTime());
      }
    }

    private boolean shouldFetchFromSchemaRegistry(String id) {
      if (!failedFetchHistories.containsKey(id)) {
        return true;
      }
      FailedFetchHistory failedFetchHistory = failedFetchHistories.get(id);
      boolean maxTriesNotExceeded = failedFetchHistory.getNumOfAttempts() < GET_SCHEMA_BY_ID_MAX_TIRES;
      boolean minRetryIntervalSatisfied =
          System.nanoTime() - failedFetchHistory.getPreviousAttemptTime() >= TimeUnit.SECONDS
              .toNanos(GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS);
      return maxTriesNotExceeded && minRetryIntervalSatisfied;
    }

    private Schema fetchSchemaByID(String id) throws SchemaNotFoundException {
      String schemaUrl = KafkaAvroSchemaRegistry.this.url + GET_RESOURCE_BY_ID + id;

      GetMethod get = new GetMethod(schemaUrl);

      int statusCode;
      String schemaString;
      try {
        statusCode = KafkaAvroSchemaRegistry.this.httpClient.executeMethod(get);
        schemaString = get.getResponseBodyAsString();
      } catch (HttpException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        get.releaseConnection();
      }

      if (statusCode != HttpStatus.SC_OK) {
        throw new SchemaNotFoundException(String.format("Schema with ID = %s cannot be retrieved", id));
      }

      Schema schema;
      if (schemaString.startsWith("{")) {
        try {
          schema = new Schema.Parser().parse(schemaString);
        } catch (Exception e) {
          throw new SchemaNotFoundException(String.format("Schema with ID = %s cannot be parsed", id));
        }
      } else {
        throw new SchemaNotFoundException(String.format("Schema with ID = %s cannot be parsed", id));
      }

      return schema;
    }

    private class FailedFetchHistory {
      private int getNumOfAttempts() {
        return numOfAttempts;
      }

      private long getPreviousAttemptTime() {
        return previousAttemptTime;
      }

      private void setPreviousAttemptTime(long previousAttemptTime) {
        this.previousAttemptTime = previousAttemptTime;
      }

      private void incrementNumOfAttempts() {
        this.numOfAttempts++;
      }

      private int numOfAttempts;
      private long previousAttemptTime;

      private FailedFetchHistory(int numOfAttempts, long previousAttemptTime) {
        this.numOfAttempts = numOfAttempts;
        this.previousAttemptTime = previousAttemptTime;
      }
    }
  }

}
