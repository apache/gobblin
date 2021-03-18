package org.apache.gobblin.test.generator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.generator.avro.AvroValueGeneratorFactory;
import org.apache.gobblin.test.generator.config.DataGeneratorConfig;


@Slf4j
public class DataGenerator implements Iterator<Object> {

  private final InMemoryFormat inMemoryFormat;
  private final boolean keepLocalCopy;
  private final long totalRecords;
  private final List replayRecords;
  private long _recordsGenerated = 0;
  private ValueGenerator valueGenerator;

  public DataGenerator(DataGeneratorConfig config) {
    inMemoryFormat = config.getInMemoryFormat();
    keepLocalCopy = config.getKeepLocalCopy();
    totalRecords = config.getTotalRecords();
    ValueGeneratorFactory factory = null;
    switch (inMemoryFormat) {
      case AVRO_GENERIC: factory = AvroValueGeneratorFactory.getInstance(); break;
      default: throw new RuntimeException(inMemoryFormat.name() + " not implemented");
    }
    try {
      valueGenerator = factory.getValueGenerator(config.getFieldConfig().getValueGen(), config.getFieldConfig());
    } catch (ValueGeneratorNotFoundException e) {
      throw new RuntimeException(e);
    }
    replayRecords = new ArrayList<Object>();
    log.info("Data generator configured with {}", config);
  }


  @Override
  public boolean hasNext() {
    if (_recordsGenerated != totalRecords) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Object next() {
    if (hasNext()) {
      _recordsGenerated++;
      Object result = this.valueGenerator.get();
      if (this.keepLocalCopy) {
        this.replayRecords.add(result);
      }
      return result;
    }
    else {
      throw new NoSuchElementException();
    }
  }

  public Object getSchema() {
    return this.valueGenerator.getPhysicalType();
  }

  public Iterator<Object> replay() {
    return this.replayRecords.iterator();
  }

}
