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

package gobblin.runtime.cli;

import java.io.IOException;

import org.apache.commons.cli.MissingOptionException;
import org.testng.Assert;
import org.testng.annotations.Test;

import lombok.Data;


public class ConstructorAndPublicMethodsCliObjectFactoryTest {

  @Test
  public void test() throws Exception {
    MyFactory factory = new MyFactory();
    MyObject object;

    try {
      // Try to build object with missing constructor argument, which is required
      object = factory.buildObject(new String[]{}, 0, false, "usage");
      Assert.fail();
    } catch (IOException exc) {
      // Expected
      Assert.assertEquals(exc.getCause().getClass(), MissingOptionException.class);
    }

    object = factory.buildObject(new String[]{"-myArg", "required"}, 0, false, "usage");
    Assert.assertEquals(object.required, "required");
    Assert.assertNull(object.string1);
    Assert.assertNull(object.string2);

    object = factory.buildObject(new String[]{"-setString1", "str1", "-myArg", "required"}, 0, false, "usage");
    Assert.assertEquals(object.required, "required");
    Assert.assertEquals(object.string1, "str1");
    Assert.assertNull(object.string2);

    object = factory.buildObject(new String[]{"-foo", "bar", "-myArg", "required"}, 0, false, "usage");
    Assert.assertEquals(object.required, "required");
    Assert.assertEquals(object.string2, "bar");
    Assert.assertNull(object.string1);

    object = factory.buildObject(new String[]{"-foo", "bar", "-setString1", "str1", "-myArg", "required"}, 0, false, "usage");
    Assert.assertEquals(object.required, "required");
    Assert.assertEquals(object.string2, "bar");
    Assert.assertEquals(object.string1, "str1");
  }

  public static class MyFactory extends ConstructorAndPublicMethodsCliObjectFactory<MyObject> {
    public MyFactory() {
      super(MyObject.class);
    }
  }

  @Data
  public static class MyObject {
    private final String required;
    private String string1;
    private String string2;

    @CliObjectSupport(argumentNames = "myArg")
    public MyObject(String required) {
      this.required = required;
    }

    @CliObjectOption(name = "foo")
    public void setString2(String str) {
      this.string2 = str;
    }
  }

}
