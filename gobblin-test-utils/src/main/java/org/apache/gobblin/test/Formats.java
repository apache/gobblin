package org.apache.gobblin.test;

import java.util.HashMap;
import java.util.Map;


/**
 * TODO: Move to runtime FormatFactory pattern: Format classes register themselves under a specific string format key.
 */
@Deprecated
public class Formats {

  public static Formats getInstance() {
    return new Formats();
  }

  Map<String, Format> formatRegistry = new HashMap<>();

  public void registerFormat(String key, Format format) {
    if (this.formatRegistry.containsKey(key)) {
      throw new RuntimeException("Duplicate registration for key " + key
          + " Existing entry is of type: " + this.formatRegistry.get(key).getClass().getCanonicalName()
          + " New entry is of type: " + format.getClass().getCanonicalName());
    }
    this.formatRegistry.put(key, format);
  }

  public Formats() {
    registerFormat(InMemoryFormat.AVRO_GENERIC.name(), new AvroGenericFormat());
    registerFormat(InMemoryFormat.POJO.name(), new PojoFormat());
    registerFormat(InMemoryFormat.JSON.name(), new JsonFormat());
  }

  public Format get(InMemoryFormat format) {
    if (this.formatRegistry.containsKey(format.name())) {
      return this.formatRegistry.get(format.name());
    } else {
      throw new RuntimeException("No format currently registered for " + format.name());
    }
  }
}
