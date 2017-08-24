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
package org.apache.gobblin.converter.json;

import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.junit.Assert;
import org.testng.annotations.Test;
import com.google.gson.JsonObject;

/**
 * Unit test for {@link BytesToJsonConverter}
 */
@Test(groups = {"gobblin.converter"})
public class BytesToJsonConverterTest {
  @Test
  public void testConverter() throws DataConversionException, IOException {
    BytesToJsonConverter converter = new BytesToJsonConverter();
    WorkUnitState state = new WorkUnitState();

    JsonObject record = converter.convertRecord("dummySchema",
        IOUtils.toByteArray(this.getClass().getResourceAsStream("/converter/jsonToAvroRecord.json")), state).iterator().next();

    Assert.assertEquals(record.get("longField").getAsLong(), 1234L);
    Assert.assertEquals(record.get("nestedRecords").getAsJsonObject().get("nestedField2").getAsString(), "test2");
  }
}
