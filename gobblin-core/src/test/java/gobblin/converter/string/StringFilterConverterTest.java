/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.converter.string;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;

import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link StringFilterConverter}.
 */
@Test(groups = { "gobblin.converter.string" })
public class StringFilterConverterTest {

  /**
   * Test for {@link StringFilterConverter#convertRecord(Class, String, WorkUnitState)} with a blank regex.
   */
  @Test
  public void testConvertRecordWithNoRegex() throws DataConversionException {
    WorkUnitState workUnitState = new WorkUnitState();
    StringFilterConverter converter = new StringFilterConverter();

    converter.init(workUnitState);

    String test = "HelloWorld";
    Iterator<String> itr = converter.convertRecord(String.class, test, workUnitState).iterator();

    Assert.assertTrue(!itr.hasNext());
  }

  /**
   * Test for {@link StringFilterConverter#convertRecord(Class, String, WorkUnitState)} with a regex that is only a
   * sequence of letters.
   */
  @Test
  public void testConvertRecordWithSimpleRegex() throws DataConversionException {
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.CONVERTER_STRING_FILTER_PATTERN, "HelloWorld");

    StringFilterConverter converter = new StringFilterConverter();

    converter.init(workUnitState);

    // Test that HelloWorld matches the pattern HelloWorld
    String test = "HelloWorld";
    Iterator<String> itr = converter.convertRecord(String.class, test, workUnitState).iterator();

    Assert.assertTrue(itr.hasNext());
    Assert.assertEquals(itr.next(), test);
    Assert.assertTrue(!itr.hasNext());

    // Test that Hello does not match the pattern HelloWorld
    test = "Hello";
    itr = converter.convertRecord(String.class, test, workUnitState).iterator();

    Assert.assertTrue(!itr.hasNext());
  }

  /**
   * Test for {@link StringFilterConverter#convertRecord(Class, String, WorkUnitState)} with a regex that actually uses
   * regex features, such as wildcards.
   */
  @Test
  public void testConvertRecordWithComplexRegex() throws DataConversionException {
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.CONVERTER_STRING_FILTER_PATTERN, ".*");

    StringFilterConverter converter = new StringFilterConverter();

    converter.init(workUnitState);

    // Test that HelloWorld matches the pattern .*
    String test = "HelloWorld";
    Iterator<String> itr = converter.convertRecord(String.class, test, workUnitState).iterator();

    Assert.assertTrue(itr.hasNext());
    Assert.assertEquals(itr.next(), test);
    Assert.assertTrue(!itr.hasNext());

    // Test that Java matches the pattern .*
    test = "Java";
    itr = converter.convertRecord(String.class, test, workUnitState).iterator();

    Assert.assertTrue(itr.hasNext());
    Assert.assertEquals(itr.next(), test);
    Assert.assertTrue(!itr.hasNext());
  }
}
