package gobblin.converter.filter;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.SchemaConversionException;

import java.io.File;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = { "gobblin.converter.filter" })
public class AvroFieldsPickConverterTest {

  private static final String PATH_PREFIX = "/home/jnchang/workspace/gobblin/gobblin/gobblin-core/src/test/resources";
  @Test
  public void testFieldsPick() throws Exception {
    Schema inputSchema = new Schema.Parser().parse(new File(PATH_PREFIX+"/converter/fieldPickInput.avsc"));

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.CONVERTER_AVRO_FIELD_PICK_FIELDS, "name,favorite_number,favorite_color");

    try (AvroFieldsPickConverter converter = new AvroFieldsPickConverter()) {
      Schema converted = converter.convertSchema(inputSchema, workUnitState);
      Schema expected = new Schema.Parser().parse(new File(PATH_PREFIX+"/converter/fieldPickExpected.avsc"));

      Assert.assertEquals(converted, expected);
    }
  }

  @Test (expectedExceptions=SchemaConversionException.class)
  public void testFieldsPickWrongFieldFailure() throws Exception {

    Schema inputSchema = new Schema.Parser().parse(new File(PATH_PREFIX+"/converter/fieldPickInput.avsc"));

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.CONVERTER_AVRO_FIELD_PICK_FIELDS, "name,favorite_number,favorite_food");

    try (AvroFieldsPickConverter converter = new AvroFieldsPickConverter()) {
      Schema converted = converter.convertSchema(inputSchema, workUnitState);
      Schema expected = new Schema.Parser().parse(new File(PATH_PREFIX+"/converter/fieldPickExpected.avsc"));

      Assert.assertEquals(converted, expected);
    }
  }
}