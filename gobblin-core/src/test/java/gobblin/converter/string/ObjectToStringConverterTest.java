package gobblin.converter.string;

import java.util.Iterator;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for {@link ObjectToStringConverter}.
 */
@Test(groups = {"gobblin.converter.string"})
public class ObjectToStringConverterTest {

  /**
   * Test for {@link ObjectToStringConverter#convertSchema(Object, WorkUnitState)}. Checks that the convertSchema method
   * always returns {@link String}.class
   */
  @Test
  public void testConvertSchema() throws SchemaConversionException {
    WorkUnitState workUnitState = new WorkUnitState();
    ObjectToStringConverter converter = new ObjectToStringConverter();

    converter.init(workUnitState);

    Assert.assertEquals(converter.convertSchema(Object.class, workUnitState), String.class);
  }

  /**
   * Test for {@link ObjectToStringConverter#convertRecord(Class, Object, WorkUnitState)}. Checks that the convertRecord
   * method properly converts an {@link Object} to its String equivalent.
   */
  @Test
  public void testConvertRecord() throws DataConversionException {
    WorkUnitState workUnitState = new WorkUnitState();
    ObjectToStringConverter converter = new ObjectToStringConverter();

    converter.init(workUnitState);

    // Test that an Integer can properly be converted to a String
    Integer integerValue = new Integer(1);
    Iterator<String> itr = converter.convertRecord(String.class, integerValue, workUnitState).iterator();

    Assert.assertTrue(itr.hasNext());
    Assert.assertEquals(itr.next(), "1");
    Assert.assertTrue(!itr.hasNext());

    // Test that a Long can properly be converted to a String
    Long longValue = new Long(2);
    itr = converter.convertRecord(String.class, longValue, workUnitState).iterator();

    Assert.assertTrue(itr.hasNext());
    Assert.assertEquals(itr.next(), "2");
    Assert.assertTrue(!itr.hasNext());
  }
}
