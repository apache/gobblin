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
package gobblin.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.annotation.Alias;

@Test(groups = { "gobblin.api.util"})
public class ClassAliasResolverTest {

  @Test
  public void testResolve() {
    ClassAliasResolver<IDummyAliasTest> resolver = new ClassAliasResolver<>(IDummyAliasTest.class);
    Assert.assertEquals(resolver.resolve("abc"), DummyAliasTest.class.getName());
    // Resolve returns the passed string if alias mapping does not exist
    Assert.assertEquals(resolver.resolve("abcd"), "abcd");
  }

  @Test
  public void testResolveClass() throws Exception {
    ClassAliasResolver<IDummyAliasTest> resolver = new ClassAliasResolver<>(IDummyAliasTest.class);

    Assert.assertEquals(resolver.resolveClass("abc"), DummyAliasTest.class);
    Assert.assertEquals(resolver.resolveClass(DummyAliasTest.class.getName()), DummyAliasTest.class);

    try {
      resolver.resolveClass("def");
      Assert.fail();
    } catch (ClassNotFoundException cnfe) {
      // expect to throw exception
    }

    try {
      resolver.resolveClass(AnotherAliasClass.class.getName());
      Assert.fail();
    } catch (ClassNotFoundException cnfe) {
      // expect to throw exception
    }
  }

  @Alias(value="abc")
  public static class DummyAliasTest implements IDummyAliasTest{}

  public static interface IDummyAliasTest {}

  @Alias(value="abc")
  public static class AnotherAliasClass {}

  @Alias(value="def")
  public static class YetAnotherAliasClass {}
}
