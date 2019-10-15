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

import org.testng.Assert;
import org.testng.annotations.Test;


public class RestrictedFieldAccessingUtilsTest {

  @Test
  public void testGetRestrictedFieldByReflection()
      throws Exception {
    BaseClass baseClass = new BaseClass(5);
    int a = (int) RestrictedFieldAccessingUtils.getRestrictedFieldByReflection(baseClass, "a", baseClass.getClass());
    Assert.assertEquals(a, 5);
  }

  @Test
  public void testGetRestrictedFieldByReflectionRecursively()
      throws Exception {
    DerivedClass derivedClass = new DerivedClass(5);
    Assert.assertEquals(derivedClass.getEnclosingValue(), 5);
    ((EnclosedClass) RestrictedFieldAccessingUtils
        .getRestrictedFieldByReflectionRecursively(derivedClass, "enclose", derivedClass.getClass())).setValue(100);
    Assert.assertEquals(derivedClass.getEnclosingValue(), 100);
  }

  @Test
  public void testNoSuchFieldException()
      throws Exception {
    DerivedClass derivedClass = new DerivedClass(5);
    try {
      RestrictedFieldAccessingUtils
          .getRestrictedFieldByReflectionRecursively(derivedClass, "non", derivedClass.getClass());
    } catch (NoSuchFieldException ne) {
      Assert.assertTrue(true);
      return;
    }

    // Should never reach here.
    Assert.assertTrue(false);
  }
}