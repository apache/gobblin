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

package gobblin.data.management.copy;

import gobblin.data.management.copy.PreserveAttributes.Option;

import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


public class PreserveAttributesTest {

  @Test
  public void test() {

    Map<String, Set<PreserveAttributes.Option>> tests = Maps.newHashMap();
    tests.put("r", Sets.newHashSet(Option.REPLICATION));
    tests.put("b", Sets.newHashSet(Option.BLOCK_SIZE));
    tests.put("u", Sets.newHashSet(Option.OWNER));
    tests.put("g", Sets.newHashSet(Option.GROUP));
    tests.put("p", Sets.newHashSet(Option.PERMISSION));
    tests.put("ru", Sets.newHashSet(Option.REPLICATION, Option.OWNER));
    tests.put("rbugp", Sets.newHashSet(Option.REPLICATION, Option.OWNER, Option.BLOCK_SIZE, Option.GROUP,
        Option.PERMISSION));
    tests.put("rrr", Sets.newHashSet(Option.REPLICATION));
    tests.put("rrb", Sets.newHashSet(Option.REPLICATION, Option.BLOCK_SIZE));
    tests.put("", Sets.<Option>newHashSet());

    for(Map.Entry<String, Set<PreserveAttributes.Option>> entry : tests.entrySet()) {
      PreserveAttributes preserve = PreserveAttributes.fromMnemonicString(entry.getKey());
      for(Option option : Option.values()) {
        Assert.assertEquals(preserve.preserve(option), entry.getValue().contains(option));
      }
      Assert.assertEquals(preserve, PreserveAttributes.fromMnemonicString(preserve.toMnemonicString()));
    }

  }

  @Test
  public void testInvalidStrings() {

    try {
      PreserveAttributes.fromMnemonicString("x");
      Assert.fail();
    } catch (IllegalArgumentException iae) {
      // Expect exception
    }

    try {
      PreserveAttributes.fromMnemonicString("rx");
      Assert.fail();
    } catch (IllegalArgumentException iae) {
      // Expect exception
    }

    try {
      PreserveAttributes.fromMnemonicString("ugq");
      Assert.fail();
    } catch (IllegalArgumentException iae) {
      // Expect exception
    }

  }

}
