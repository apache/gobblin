package org.apache.gobblin.metadata.types;

import java.util.HashMap;
import java.util.Map;


/**
 * Represents a collection of global and record-level metadata that can be
 * attached to a record as it flows through a Gobblin pipeline.
 */
public class Metadata {
  private GlobalMetadata globalMetadata;
  private Map<String, Object> recordMetadata;

  public Metadata() {
    globalMetadata = new GlobalMetadata();
    recordMetadata = new HashMap<>();
  }

  public GlobalMetadata getGlobalMetadata() {
    return globalMetadata;
  }

  public Map<String, Object> getRecordMetadata() {
    return recordMetadata;
  }
}
