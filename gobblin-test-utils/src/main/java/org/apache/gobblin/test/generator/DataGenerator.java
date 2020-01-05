package org.apache.gobblin.test.generator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.gobblin.test.Format;
import org.apache.gobblin.test.Formats;


public class DataGenerator implements Iterator<Object> {

  final Formats.InMemoryFormat inMemoryFormat;
  final boolean keepLocalCopy;
  final long totalRecords;
  final Format format;
  private List<Field> fields = new ArrayList<>();
  private long _recordsGenerated = 0;
  private Object schema = null;


  public DataGenerator(DataGeneratorConfig config)
      throws ClassNotFoundException {
    inMemoryFormat = config.getInMemoryFormat();
    keepLocalCopy = false;
    totalRecords = config.getTotalRecords();
    /**


    fields = new ArrayList<>();
    for (FieldConfig fieldConfig : config.getSchema().getFields()) {
      String valueGeneratorName = fieldConfig.getValueGen();
      Config valueGeneratorConfig = fieldConfig.valueGenConfig;
      if (valueGeneratorConfig == null) {
        valueGeneratorConfig = ConfigFactory.empty();
      }
      // Get the registered value generator for this name and target type
      ValueGenerator valueGenerator = ValueGeneratorFactory.getInstance()
          .getValueGenerator(valueGeneratorName, fieldConfig);
      Field field = new Field(fieldConfig.name, fieldConfig.type, fieldConfig.optional, valueGenerator, null);
      fields.add(field);
    }
     **/
    format = Formats.getInstance().get(this.inMemoryFormat);
  }

  public DataGenerator(Formats.InMemoryFormat inMemoryFormat, long totalRecords, boolean keepLocalCopy,
      List<Field> fields) {
    this.inMemoryFormat = inMemoryFormat == null? Formats.InMemoryFormat.AVRO_GENERIC : inMemoryFormat;
    this.totalRecords = totalRecords == 0? 1: totalRecords;
    this.keepLocalCopy = keepLocalCopy;
    this.format = Formats.getInstance().get(this.inMemoryFormat);
    this.fields = fields;
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
    return this.format.generateRandomRecord();
  }

  public Object getSchema() {
    this.schema = this.format.generateRandomSchema(this.fields);
    return this.schema;
  }

  public static DataGenerator.Builder builder() {
    return new DataGenerator.Builder();
  }

  public static class Builder {

    private long totalRecords = 0;
    private boolean keepLocalCopy = false;
    private Formats.InMemoryFormat inMemoryFormat;
    private List<Field> fieldList = new ArrayList();

    public Builder inMemoryFormat(Formats.InMemoryFormat inMemoryFormat) {
      this.inMemoryFormat = inMemoryFormat;
      return this;
    }

    public Builder totalRecords(long totalRecords) {
      this.totalRecords = totalRecords;
      return this;
    }

    public Builder keepLocalCopy(boolean keepLocalCopy) {
      this.keepLocalCopy = keepLocalCopy;
      return this;
    }

    public Builder withField(Field singleField) {
      this.fieldList.add(singleField);
      return this;
    }

    public DataGenerator build() {
      return new DataGenerator(this.inMemoryFormat,
          this.totalRecords,
          this.keepLocalCopy,
          this.fieldList);
    }


  }

}
