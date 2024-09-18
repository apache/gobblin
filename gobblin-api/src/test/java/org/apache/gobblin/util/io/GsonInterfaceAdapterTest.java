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

package org.apache.gobblin.util.io;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.gobblin.util.test.BaseClass;
import org.apache.gobblin.util.test.TestClass;


public class GsonInterfaceAdapterTest {

  @Test(groups = {"gobblin.util.io"})
  public void test() {
    Gson gson = GsonInterfaceAdapter.getGson(Object.class);

    TestClass test = new TestClass();
    test.absent = Optional.absent();
    Assert.assertNotEquals(test, new TestClass());

    String ser = gson.toJson(test);
    BaseClass deser = gson.fromJson(ser, BaseClass.class);
    Assert.assertEquals(test, deser);

  }

  @Test
  public void testObjectToIntegerDeserialize() {
    Gson gson = new GsonBuilder().create();
    Integer test = 5;
    String ser = gson.toJson(test);
    Object deser = gson.fromJson(ser, Object.class);
    Assert.assertNotEquals(test, deser);
    Assert.assertTrue(deser instanceof Double);

    Gson customGson = new GsonBuilder()
        .setObjectToNumberStrategy(GsonInterfaceAdapter.CustomToNumberPolicy.INTEGER_OR_LONG_OR_DOUBLE)
        .create();

    Object deser2 = customGson.fromJson(ser, Object.class);
    Assert.assertEquals(test, deser2);
    Assert.assertTrue(deser2 instanceof Integer);
  }

  @Test
  public void testObjectToLongDeserialize() {
    Gson gson = new GsonBuilder().create();
    Long test = 1234567890123456789L;
    String ser = gson.toJson(test);
    Object deser = gson.fromJson(ser, Object.class);
    Assert.assertNotEquals(test, deser);
    Assert.assertTrue(deser instanceof Double);

    Gson customGson = new GsonBuilder()
        .setObjectToNumberStrategy(GsonInterfaceAdapter.CustomToNumberPolicy.INTEGER_OR_LONG_OR_DOUBLE)
        .create();

    Object deser2 = customGson.fromJson(ser, Object.class);
    Assert.assertEquals(test, deser2);
    Assert.assertTrue(deser2 instanceof Long);
  }

  @Test
  public void testObjectToDoubleDeserialize() {
    Gson gson = new GsonBuilder().create();
    Double test = 5.0;
    String ser = gson.toJson(test);
    Object deser = gson.fromJson(ser, Object.class);
    Assert.assertEquals(test, deser);
    Assert.assertTrue(deser instanceof Double);

    Gson customGson = new GsonBuilder()
        .setObjectToNumberStrategy(GsonInterfaceAdapter.CustomToNumberPolicy.INTEGER_OR_LONG_OR_DOUBLE)
        .create();

    Object deser2 = customGson.fromJson(ser, Object.class);
    Assert.assertEquals(test, deser2);
    Assert.assertTrue(deser2 instanceof Double);
  }

}
