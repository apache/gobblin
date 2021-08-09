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

package org.apache.gobblin.data.management.dataset;

import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.util.filters.RegexPathFilter;


public class PathFilterTest {

  @Test
  public void testRegexFilter() {
    Path unmatchedPath = new Path(".abc");
    Path matchedPath1 = new Path("abc");
    Path matchedPath2 = new Path("a.bc");
    Properties props = new Properties();
    props.setProperty(DatasetUtils.PATH_FILTER_KEY, RegexPathFilter.class.getName());
    props.setProperty(DatasetUtils.CONFIGURATION_KEY_PREFIX + RegexPathFilter.REGEX, "^[^.].*"); // match everything that does not start with a dot

    PathFilter includeFilter = DatasetUtils.instantiatePathFilter(props);

    Assert.assertFalse(includeFilter.accept(unmatchedPath));
    Assert.assertTrue(includeFilter.accept(matchedPath1));
    Assert.assertTrue(includeFilter.accept(matchedPath2));
  }
}
