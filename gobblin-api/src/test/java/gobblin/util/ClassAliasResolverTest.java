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
package gobblin.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.annotation.Alias;

@Test(groups = { "gobblin.api.util"})
public class ClassAliasResolverTest {

  @Test
  public void testResolve() {
    ClassAliasResolver resolver = new ClassAliasResolver(IDummyAliasTest.class);
    Assert.assertEquals(resolver.resolve("abc"), DummyAliasTest.class.getCanonicalName());
    // Resolve returns the passed string if alias mapping does not exist
    Assert.assertEquals(resolver.resolve("abcd"), "abcd");
  }

  @Alias(value="abc")
  public static class DummyAliasTest implements IDummyAliasTest{}

  public static interface IDummyAliasTest {}
}
