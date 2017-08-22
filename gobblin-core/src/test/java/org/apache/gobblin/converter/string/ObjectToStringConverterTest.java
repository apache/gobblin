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

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link ObjectToStringConverter}.
 */
@Test(groups = { "gobblin.converter.string" })
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
