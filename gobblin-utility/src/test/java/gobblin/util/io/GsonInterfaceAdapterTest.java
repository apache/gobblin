/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.io;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.gson.Gson;

import gobblin.util.test.BaseClass;
import gobblin.util.test.TestClass;


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

}
