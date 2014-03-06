package com.linkedin.uif.converter.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.lumos.util.SchemaAttributes;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.LumosAttributesConverter;
import com.linkedin.uif.source.workunit.Extract;
import com.linkedin.uif.source.workunit.Extract.TableType;


/**
 * Unit test for {@link LumosAttributesConverter}
 * @author kgoodhop
 *
 */
@Test(groups = { "com.linkedin.uif.converter" })
public class TestLumosAttributesConverter {
  private Schema schema;
  private WorkUnitState state;
  private WorkUnitState state2;
  private String extractId = "20140302202356";
  private long fullRuntime = 1393905086000l;
  private long validationRecordCount = 1000;
  private long validationCountHWM = 1393705086000l;

  @BeforeClass
  public void setUp() throws Exception {
    Schema.Parser parser = new Schema.Parser();
    
    schema = parser.parse(this.getClass().getResourceAsStream("/avro.avsc"));
       
    SourceState source = new SourceState();
    Extract extract = source.createExtract(TableType.SNAPSHOT_ONLY, "test_namespace",
        "test_table", String.valueOf(extractId));
    
    extract.setPrimaryKeys("Id");
    extract.setDeltaFields("LastModifiedDate");  
    extract.setValidationRecordCount(validationRecordCount, validationCountHWM);
    extract.setIsSecured("sgroup");
    state = new WorkUnitState(source.createWorkUnit(extract));
    
    extract = new Extract(extract);
    extract.setFullTrue(fullRuntime);
    
    state2 = new WorkUnitState(source.createWorkUnit(extract));
    
  }

  @Test
  public void testConverter() throws Exception {
    Converter<Schema, Schema, GenericRecord, GenericRecord> converter = new LumosAttributesConverter();
    
    Schema newSchema = converter.convertSchema(schema, state);
    
    SchemaAttributes att = new SchemaAttributes(newSchema);
    
    Assert.assertEquals(att.getDropDate(), 1393791836000l);
    Assert.assertEquals(att.getPrimaryKeyFields()[0], "Id");
    Assert.assertEquals(att.getDeltaField(), "LastModifiedDate");

    Assert.assertEquals(att.getValidationCount(), validationRecordCount);
    Assert.assertEquals(att.getValidationHighWater(), validationCountHWM);
    Assert.assertEquals(att.isSecured(), true);
    Assert.assertEquals(att.getPermissionGroup(), "sgroup");
    
    att = new SchemaAttributes(converter.convertSchema(schema, state2));
    System.out.println(att.getSchema());
    Assert.assertEquals(att.getFullDropDate(), fullRuntime);

  }
}
