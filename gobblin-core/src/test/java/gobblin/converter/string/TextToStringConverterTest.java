/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.converter.string;

import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;


/**
 * Unit tests for {@link TextToStringConverter}.
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.converter.string" })
public class TextToStringConverterTest {

  @Test
  public void testConvertRecord() throws DataConversionException {
    TextToStringConverter textToStringConverter =
        (TextToStringConverter) new TextToStringConverter().init(new WorkUnitState());
    Text text = new Text("test");
    Iterator<String> iterator = textToStringConverter.convertRecord(null, text, new WorkUnitState()).iterator();
    Assert.assertTrue(iterator.hasNext());
    String textString = iterator.next();
    Assert.assertEquals(textString, text.toString());
    Assert.assertFalse(iterator.hasNext());
  }
}
