package gobblin.converter.string;

import java.util.Iterator;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for {@link StringSplitterConverter}.
 */
@Test(groups = {"gobblin.converter.string"})
public class StringSplitterConverterTest {

  /**
   * Test that {@link StringSplitterConverter#init(WorkUnitState)} throws an {@link IllegalArgumentException} if the
   * parameter {@link ConfigurationKeys#CONVERTER_STRING_SPLITTER_DELIMITER} is not specified in the config.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInit() {
    WorkUnitState workUnitState = new WorkUnitState();
    StringSplitterConverter converter = new StringSplitterConverter();
    converter.init(workUnitState);
  }

  /**
   * Test that {@link StringSplitterConverter#convertRecord(Class, String, WorkUnitState)} properly splits a String by
   * a specified delimiter.
   */
  @Test
  public void testConvertRecord() throws DataConversionException {
    String delimiter = "\t";
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.CONVERTER_STRING_SPLITTER_DELIMITER, delimiter);

    StringSplitterConverter converter = new StringSplitterConverter();
    converter.init(workUnitState);

    // Test that the iterator returned by convertRecord is of length 1 when the delimiter is not in the inputRecord
    String test = "HelloWorld";
    Iterator<String> itr = converter.convertRecord(String.class, test, workUnitState).iterator();

    Assert.assertTrue(itr.hasNext());
    Assert.assertEquals(itr.next(), test);
    Assert.assertTrue(!itr.hasNext());

    // Test that the iterator returned by convertRecord is of length 2 when the delimiter is in the middle of two strings
    String test1 = "Hello";
    String test2 = "World";
    test = test1 + delimiter + test2;
    itr = converter.convertRecord(String.class, test, workUnitState).iterator();

    Assert.assertTrue(itr.hasNext());
    Assert.assertEquals(itr.next(), test1);
    Assert.assertTrue(itr.hasNext());
    Assert.assertEquals(itr.next(), test2);
    Assert.assertTrue(!itr.hasNext());

    // Test that the iterator returned by convertRecord is of length 2 even when the delimiter occurs multiple times in
    // between the same two strings, and if the delimiter occurs at the end and beginning of the inputRecord
    test1 = "Hello";
    test2 = "World";
    test = delimiter + test1 + delimiter + delimiter + test2 + delimiter;
    itr = converter.convertRecord(String.class, test, workUnitState).iterator();

    Assert.assertTrue(itr.hasNext());
    Assert.assertEquals(itr.next(), test1);
    Assert.assertTrue(itr.hasNext());
    Assert.assertEquals(itr.next(), test2);
    Assert.assertTrue(!itr.hasNext());
  }
}
