package gobblin.source.extractor.extract.kafka;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.repo.SchemaEntry;
import org.apache.avro.repo.SchemaValidationException;
import org.apache.avro.repo.Subject;
import org.apache.avro.repo.client.RESTRepositoryClient;

import com.google.common.collect.Maps;


/**
 * A schema registry class that provides two services: get the latest schema of a topic, and register a schema.
 *
 * @author ziliu
 *
 */
public class KafkaAvroSchemaRegistry {

  private static final String SCHEMA_VALIDATOR_CLASS = "org.apache.avro.repo.Validator";
  private static final int GET_SCHEMA_BY_ID_MAX_TIRES = 3;
  private static final int GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS = 1;

  private final Map<String, Schema> cachedSchemas;
  private final Map<String, FailedFetchHistory> failedFetchHistories;

  private final RESTRepositoryClient client;

  public KafkaAvroSchemaRegistry(String url) {
    this.client = new RESTRepositoryClient(url);
    this.cachedSchemas = Maps.newConcurrentMap();
    this.failedFetchHistories = Maps.newHashMap();
  }

  public Schema getLatestSchemaByTopic(String topic) throws SchemaNotFoundException {
    if (this.cachedSchemas.containsKey(topic)) {
      return this.cachedSchemas.get(topic);
    }
    if (shouldFetchFromSchemaRegistry(topic)) {
      Schema schema = fetchLatestSchemaByTopic(topic);
      this.cachedSchemas.putIfAbsent(topic, schema);
      return schema;
    } else {
      throw new SchemaNotFoundException(String.format("Latest schema for topic %s cannot be retrieved", topic));
    }
  }

  private boolean shouldFetchFromSchemaRegistry(String topic) {
    if (!failedFetchHistories.containsKey(topic)) {
      return true;
    }
    FailedFetchHistory failedFetchHistory = failedFetchHistories.get(topic);
    boolean maxTriesNotExceeded = failedFetchHistory.numOfAttempts < GET_SCHEMA_BY_ID_MAX_TIRES;
    boolean minRetryIntervalSatisfied =
        System.nanoTime() - failedFetchHistory.previousAttemptTime >= TimeUnit.SECONDS
            .toNanos(GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS);
    return maxTriesNotExceeded && minRetryIntervalSatisfied;
  }

  private Schema fetchLatestSchemaByTopic(String topic) throws SchemaNotFoundException {
    Subject subject = client.lookup(topic);
    if (subject == null) {
      throw new SchemaNotFoundException(String.format("Latest schema for topic %s cannot be retrieved", topic));
    }

    SchemaEntry entry = subject.latest();

    if (entry == null)
      throw new SchemaNotFoundException(String.format("Latest schema for topic %s cannot be retrieved", topic));
    return new Schema.Parser().parse(entry.getSchema());
  }

  public String register(String topic, Schema schema) throws SchemaValidationException {
    Subject subject = client.lookup(topic);

    if (subject == null) {
      subject = client.register(topic, SCHEMA_VALIDATOR_CLASS);
    }

    return subject.register(schema.toString()).getId();
  }

  private class FailedFetchHistory {
    private int numOfAttempts;
    private long previousAttemptTime;

    private FailedFetchHistory(int numOfAttempts, long previousAttemptTime) {
      this.numOfAttempts = numOfAttempts;
      this.previousAttemptTime = previousAttemptTime;
    }
  }
}
