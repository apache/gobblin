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

package org.apache.gobblin.util.filters;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Unit test for {@link RegexPathFilter}.
 */
@Test(groups = { "gobblin.util.filters" })
public class RegexPathFilterTest {

  @Test
  public void testAccept() {
    String regex = "a.*\\.b";
    Path matchedPath = new Path("fsuri://testdir/subdir/a11.b");
    Path unmatchedPath = new Path("fsuri://testdir/subdir/a.11b");
    RegexPathFilter includeFilter = new RegexPathFilter(regex);
    RegexPathFilter excludeFilter = new RegexPathFilter(regex, false);
    Assert.assertTrue(includeFilter.accept(matchedPath));
    Assert.assertFalse(includeFilter.accept(unmatchedPath));
    Assert.assertFalse(excludeFilter.accept(matchedPath));
    Assert.assertTrue(excludeFilter.accept(unmatchedPath));
  }
}