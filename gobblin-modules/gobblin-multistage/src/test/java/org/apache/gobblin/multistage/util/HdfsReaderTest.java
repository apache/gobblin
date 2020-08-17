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

package org.apache.gobblin.multistage.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.multistage.util.HdfsReader;
import org.apache.gobblin.multistage.util.JsonSchemaGenerator;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link JsonSchemaGenerator}
 * @author chrli
 *
 */
@Test(groups = {"org.apache.gobblin.util"})
public class HdfsReaderTest {
  private Gson gson;
  private State state;
  private HdfsReader reader;

  @BeforeClass
  public void setup() {
    gson = new Gson();
    state = new State();
    reader = new HdfsReader(state, new JsonArray());
  }

  @Test
  public void testValidInitializationAndFieldsProcessing() {
    JsonArray jsonArray = gson.fromJson(
        new InputStreamReader(this.getClass().getResourceAsStream("/util/surveyids.json")), JsonArray.class);

    SourceState state = new SourceState();
    JsonArray secondaryInput = gson.fromJson("[{\"path\": \"" +
        this.getClass().getResource("/util/avro").toString() + "\", \"fields\": [\"id\"]}]", JsonArray.class);
    HdfsReader reader = new HdfsReader(state, secondaryInput);

    Assert.assertEquals(jsonArray.toString(), reader.readAll().get("activation").toString());
  }

  @Test
  public void testFiltersOnSecondaryInput() {
    String expected = "[{\"id\":\"179513\"},{\"id\":\"179758\"},{\"id\":\"179895\"}]";
    SourceState state = new SourceState();
    JsonArray secondaryInput = gson.fromJson("[{\"path\": \""
        + this.getClass().getResource("/util/avro").toString()
        + "\", \"fields\": [\"id\"], \"filters\": {\"id\": \"17.*\"}}]", JsonArray.class);
    HdfsReader reader = new HdfsReader(state, secondaryInput);
    Assert.assertEquals(reader.readAll().get("activation").toString(), expected);
  }

  /**
   * Test readAll with null/empty JsonArry
   * Expect: empty HashMap
   */
  @Test
  public void testReadAll() {
    reader = new HdfsReader(state, null);
    Assert.assertEquals(reader.readAll(), Collections.emptyMap());

    reader = new HdfsReader(state, gson.fromJson("[]", JsonArray.class));
    Assert.assertEquals(reader.readAll(), Collections.emptyMap());
  }

  /**
   * Test createDataReader with invalid path
   */
  @Test
  public void testCreateDataReaderWithInvalidPath() throws NoSuchMethodException {
    Method method = HdfsReader.class.getDeclaredMethod("createDataReader", String.class);
    method.setAccessible(true);
    try {
      method.invoke(reader, "");
    } catch (InvocationTargetException | IllegalAccessException e) {
      Assert.assertEquals(e.getCause().getClass(), RuntimeException.class);
    }
  }

  /**
   * Test readRecordsFromPath with invalid path
   */
  @Test
  public void testReadRecordsFromPathWithInvalidPath() throws NoSuchMethodException {
    Method method = HdfsReader.class.getDeclaredMethod("readRecordsFromPath", String.class, List.class, Map.class);
    method.setAccessible(true);
    try {
      method.invoke(reader, "location", Collections.emptyList(), Collections.emptyMap());
    } catch (InvocationTargetException | IllegalAccessException e) {
      Assert.assertEquals(e.getCause().getClass(), RuntimeException.class);
    }
  }

  /**
   * Test getFieldsAsList with no 'fields' JsonElement
   * Expect: empty list
   */
  @Test
  public void testGetFieldsAsList() {
    Assert.assertEquals(reader.getFieldsAsList(gson.fromJson("{\"no_fields\": \"testing_path\"}", JsonElement.class)),
        Collections.emptyList());
  }

  /**
   * Test toJsonArray with valid input
   */
  @Test
  public void testToJsonArrayWithValidInput() {
    String validSecondaryInput = "[{\"path\": \"testing_path\", \"fields\": [\"s3key\"]}]";
    Assert.assertEquals(reader.toJsonArray(validSecondaryInput), gson.fromJson(validSecondaryInput, JsonArray.class));
  }

  /**
   * Test toJsonArray with invalid input
   * Expected: RuntimeException
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testToJsonArrayWithInvalidInput() {
    reader.toJsonArray("{\"path\": \"testing_path\"}");
  }
}