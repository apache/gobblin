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
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;


/**
 * Tests for {@link StringSplitterToListConverter}.
 */
@Test(groups = {"gobblin.converter.string"})
public class StringSplitterToListConverterTest {

  @Test
  public void testConvertRecord()
      throws DataConversionException {
    StringSplitterToListConverter converter = new StringSplitterToListConverter();
    String delimiter1 = "sep";
    WorkUnitState workUnitState1 = new WorkUnitState();
    workUnitState1.setProp(ConfigurationKeys.CONVERTER_STRING_SPLITTER_DELIMITER, delimiter1);
    workUnitState1.setProp(ConfigurationKeys.CONVERTER_STRING_SPLITTER_SHOULD_TRIM_RESULTS, true);
    converter.init(workUnitState1);
    String inputRecord1 = "1sep2sepsep";
    Iterator<List<String>> recordIterator = converter.convertRecord("", inputRecord1, workUnitState1).iterator();
    Assert.assertTrue(recordIterator.hasNext());
    List<String> record1 = recordIterator.next();
    Assert.assertEquals(record1, Lists.newArrayList("1", "2"));
    Assert.assertFalse(recordIterator.hasNext());

    workUnitState1.setProp(ConfigurationKeys.CONVERTER_STRING_SPLITTER_SHOULD_TRIM_RESULTS, false);
    converter.init(workUnitState1);
    recordIterator = converter.convertRecord("", inputRecord1, workUnitState1).iterator();
    Assert.assertTrue(recordIterator.hasNext());
    record1 = recordIterator.next();
    Assert.assertEquals(record1, Lists.newArrayList("1", "2", "", ""));
    Assert.assertFalse(recordIterator.hasNext());

    String delimiter2 = "\n\t";
    String inputRecord2 = "1" + delimiter2 + "2" + delimiter2 + " ";
    WorkUnitState workUnitState2 = new WorkUnitState();
    workUnitState2.setProp(ConfigurationKeys.CONVERTER_STRING_SPLITTER_DELIMITER, delimiter2);
    converter.init(workUnitState2);
    recordIterator = converter.convertRecord("", inputRecord2, workUnitState2).iterator();
    Assert.assertTrue(recordIterator.hasNext());
    List<String> record2 = recordIterator.next();
    Assert.assertEquals(record2, Lists.newArrayList("1", "2", " "));
    Assert.assertFalse(recordIterator.hasNext());
  }
}
