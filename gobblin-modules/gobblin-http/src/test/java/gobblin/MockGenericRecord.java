package gobblin;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class MockGenericRecord implements GenericRecord {
  private final Map<String, Object> map;

  public MockGenericRecord() {
    map = new HashMap<>();
  }

  @Override
  public void put(String key, Object v) {
    map.put(key, v);
  }

  @Override
  public Object get(String key) {
    return map.get(key);
  }

  @Override
  public void put(int i, Object v) {
    throw new UnsupportedOperationException("Put by index not supported");
  }

  @Override
  public Object get(int i) {
    throw new UnsupportedOperationException("Get by index not supported");
  }

  @Override
  public Schema getSchema() {
    throw new UnsupportedOperationException("Get schema not supported");
  }
}
