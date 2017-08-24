package org.apache.gobblin.converter.avro;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.NullConverter;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.RecordConverter;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testng.Assert;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.ArrayConverter;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.ARRAY;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.NULL;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.RECORD;


public class JsonElementConversionFactoryTest {

  private static WorkUnitState state;

  @BeforeClass
  public static void setUp() {
    WorkUnit workUnit = new WorkUnit(new SourceState(),
        new Extract(new SourceState(), Extract.TableType.SNAPSHOT_ONLY, "namespace", "dummy_table"));
    state = new WorkUnitState(workUnit);
  }

  @Test
  public void schemaWithArrayOfMaps()
      throws Exception {
    String schema =
        "{\"columnName\":\"b\",\"dataType\":{\"type\":\"array\", \"items\":{\"dataType\":{\"type\":\"map\", \"values\":\"string\"}}}}";
    String expected =
        "{\"type\":\"array\",\"items\":{\"type\":\"map\",\"values\":{\"type\":\"string\",\"source.type\":\"string\"},\"source.type\":\"map\"},\"source.type\":\"array\"}";

    ArrayConverter arrayConverter = new ArrayConverter("dummy", true, ARRAY.toString(), buildJsonObject(schema), state);

    Assert.assertEquals(arrayConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithArrayOfRecords()
      throws Exception {
    String schema =
        "{\"columnName\":\"b\", \"dataType\":{\"type\":\"array\", \"items\":{\"dataType\":{\"type\":\"record\", \"namespace\":\"org.foo\", \"values\":[{\"columnName\": \"name\", \"dataType\":{\"type\":\"string\"}},{\"columnName\": \"c\", \"dataType\":{\"type\":\"long\"}},{\"columnName\": \"cc\", \"dataType\":{\"type\":\"array\", \"items\":\"int\"}}]}}}}";
    String expected =
        "{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"dummy_table\",\"namespace\":\"namespace\",\"doc\":\"\",\"fields\":[{\"name\":\"name\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"\",\"source.type\":\"string\"},{\"name\":\"c\",\"type\":{\"type\":\"long\",\"source.type\":\"long\"},\"doc\":\"\",\"source.type\":\"long\"},{\"name\":\"cc\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"int\",\"source.type\":\"int\"},\"source.type\":\"array\"},\"doc\":\"\",\"source.type\":\"array\"}],\"source.type\":\"record\"},\"source.type\":\"array\"}";

    ArrayConverter arrayConverter =
        new ArrayConverter("dummy1", true, ARRAY.toString(), buildJsonObject(schema), state);

    Assert.assertEquals(arrayConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithRecord()
      throws DataConversionException, SchemaConversionException, UnsupportedDateTypeException {
    String schemaStr =
        "{\"columnName\":\"b\", \"dataType\":{\"type\":\"record\", \"values\":[{\"columnName\":\"c\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"d\",\"dataType\":{\"type\":\"int\"}}]}}";
    String expected =
        "{\"type\":\"record\",\"name\":\"dummy_table\",\"namespace\":\"namespace\",\"doc\":\"\",\"fields\":[{\"name\":\"c\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"\",\"source.type\":\"string\"},{\"name\":\"d\",\"type\":{\"type\":\"int\",\"source.type\":\"int\"},\"doc\":\"\",\"source.type\":\"int\"}],\"source.type\":\"record\"}";

    RecordConverter recordConverter =
        new RecordConverter("dummy1", true, RECORD.toString(), buildJsonObject(schemaStr), state);

    Assert.assertEquals(recordConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithArrayOfInts()
      throws Exception {
    String schemaStr = "{\"columnName\":\"b\", \"dataType\":{\"type\":\"array\", \"items\":\"int\"}}";
    String expected =
        "{\"type\":\"array\",\"items\":{\"type\":\"int\",\"source.type\":\"int\"},\"source.type\":\"array\"}";

    ArrayConverter arrayConverter =
        new ArrayConverter("dummy1", true, ARRAY.toString(), buildJsonObject(schemaStr), state);

    Assert.assertEquals(arrayConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithNullType() {
    NullConverter nullConverter = new NullConverter("dummy1", ARRAY.toString());

    Assert.assertEquals(nullConverter.schema().toString(), "{\"type\":\"null\",\"source.type\":\"array\"}");
  }

  @Test
  public void schemaWithArrayOfEnums()
      throws Exception {
    String schemaStr =
        "{\"columnName\":\"b\", \"dataType\":{\"type\":\"array\", \"items\":{\"dataType\":{\"type\":\"enum\", \"namespace\":\"org.foo\", \"dataType\":{\"name\":\"choice\", \"symbols\":[\"YES\", \"NO\"]}}}}}";
    String expected =
        "{\"type\":\"array\",\"items\":{\"type\":\"enum\",\"name\":\"choice\",\"doc\":\"\",\"symbols\":[\"YES\",\"NO\"],\"source.type\":\"enum\"},\"source.type\":\"array\"}";
    ArrayConverter arrayConverter =
        new ArrayConverter("dummy1", true, ARRAY.toString(), buildJsonObject(schemaStr), state);

    Assert.assertEquals(arrayConverter.schema().toString(), expected);
  }

  private static JsonObject buildJsonObject(String jsonStr) {
    JsonParser parser = new JsonParser();
    return parser.parse(jsonStr).getAsJsonObject();
  }
}