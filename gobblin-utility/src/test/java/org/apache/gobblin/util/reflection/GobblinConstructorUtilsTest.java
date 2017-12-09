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

package org.apache.gobblin.util.reflection;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;


public class GobblinConstructorUtilsTest {

  @Test
  public void testInvokeFirst()
      throws Exception {

    ConstructorTestClass ctc = GobblinConstructorUtils.invokeFirstConstructor(ConstructorTestClass.class,
        ImmutableList.<Object>of(Integer.valueOf(3), new Properties()),
        ImmutableList.<Object>of(Integer.valueOf(3), "test1"));

    Assert.assertNotNull(ctc.id);
    Assert.assertNotNull(ctc.props);
    Assert.assertEquals(ctc.id, Integer.valueOf(3));

    Assert.assertNull(ctc.str);

    ctc = GobblinConstructorUtils
        .invokeFirstConstructor(ConstructorTestClass.class, ImmutableList.<Object>of(Integer.valueOf(3), "test1"),
            ImmutableList.<Object>of(Integer.valueOf(3), new Properties()));

    Assert.assertNotNull(ctc.id);
    Assert.assertNotNull(ctc.str);
    Assert.assertEquals(ctc.id, Integer.valueOf(3));
    Assert.assertEquals(ctc.str, "test1");

    Assert.assertNull(ctc.props);
  }

  @Test(expectedExceptions = NoSuchMethodException.class)
  public void testInvokeFirstException()
      throws Exception {
    GobblinConstructorUtils.invokeFirstConstructor(ConstructorTestClass.class, ImmutableList.<Object>of(),
        ImmutableList.<Object>of(Integer.valueOf(3), Integer.valueOf(3)));
  }

  public static class ConstructorTestClass {
    Properties props = null;
    String str = null;
    Integer id = null;

    public ConstructorTestClass(Integer id, Properties props) {
      this.id = id;
      this.props = props;
    }

    public ConstructorTestClass(Integer id, String str) {
      this.id = id;
      this.str = str;
    }
  }

  public static class ConstructorTestClassWithNoArgs extends ConstructorTestClass {
    public ConstructorTestClassWithNoArgs() {
      super(0, "noArgs");
    }
  }

  @Test
  public void testLongestConstructor()
      throws Exception {

    ConstructorTestClass obj = GobblinConstructorUtils
        .invokeLongestConstructor(ConstructorTestClass.class, Integer.valueOf(1), "String1", "String2");
    Assert.assertEquals(obj.id.intValue(), 1);
    Assert.assertEquals(obj.str, "String1");

    obj = GobblinConstructorUtils.invokeLongestConstructor(ConstructorTestClass.class, Integer.valueOf(1), "String1");
    Assert.assertEquals(obj.id.intValue(), 1);
    Assert.assertEquals(obj.str, "String1");

    try {
      obj = GobblinConstructorUtils.invokeLongestConstructor(ConstructorTestClass.class, Integer.valueOf(1));
      Assert.fail();
    } catch (NoSuchMethodException nsme) {
      //expected to throw exception
    }

    obj = GobblinConstructorUtils.invokeLongestConstructor(ConstructorTestClassWithNoArgs.class);
    Assert.assertEquals(obj.id.intValue(), 0);
    Assert.assertEquals(obj.str, "noArgs");
    Assert.assertNull(obj.props);

    obj = GobblinConstructorUtils.invokeLongestConstructor(ConstructorTestClassWithNoArgs.class, Integer.valueOf(1));
    Assert.assertEquals(obj.id.intValue(), 0);
    Assert.assertEquals(obj.str, "noArgs");
    Assert.assertNull(obj.props);
  }
}
