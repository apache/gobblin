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

package org.apache.gobblin.converter.string;

import java.util.Iterator;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link StringSplitterConverter}.
 */
@Test(groups = { "gobblin.converter.string" })
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
