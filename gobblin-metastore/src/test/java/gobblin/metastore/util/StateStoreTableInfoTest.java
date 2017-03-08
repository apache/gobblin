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

package gobblin.metastore.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class StateStoreTableInfoTest {
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void getNull() {
    StateStoreTableInfo.get(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void getEmpty() {
    StateStoreTableInfo.get("");
  }

  @Test
  public void getSeperator() {
    StateStoreTableInfo info = StateStoreTableInfo.get("-");
    Assert.assertEquals(info.getPrefix(), "");
    Assert.assertFalse(info.isCurrent());
  }

  @Test
  public void getCurrent() {
    StateStoreTableInfo info = StateStoreTableInfo.get("current");
    Assert.assertEquals(info.getPrefix(), "");
    Assert.assertTrue(info.isCurrent());
  }

  @Test
  public void getCurrentWithWeirdCasing() {
    StateStoreTableInfo info = StateStoreTableInfo.get("CuRrenT");
    Assert.assertEquals(info.getPrefix(), "");
    Assert.assertTrue(info.isCurrent());
  }

  @Test
  public void getCurrentWithExtension() {
    StateStoreTableInfo info = StateStoreTableInfo.get("current.jst");
    Assert.assertEquals(info.getPrefix(), "");
    Assert.assertTrue(info.isCurrent());
  }

  @Test
  public void getOld() {
    StateStoreTableInfo info = StateStoreTableInfo.get("old.jst");
    Assert.assertEquals(info.getPrefix(), "");
    Assert.assertFalse(info.isCurrent());
  }

  @Test
  public void getOldEmptyPrefix() {
    StateStoreTableInfo info = StateStoreTableInfo.get("-old.jst");
    Assert.assertEquals(info.getPrefix(), "");
    Assert.assertFalse(info.isCurrent());
  }

  @Test
  public void getOldWithPrefix() {
    StateStoreTableInfo info = StateStoreTableInfo.get("prefix-old.jst");
    Assert.assertEquals(info.getPrefix(), "prefix");
    Assert.assertFalse(info.isCurrent());
  }

  @Test
  public void getCurrentEmptyPrefix() {
    StateStoreTableInfo info = StateStoreTableInfo.get("-current.jst");
    Assert.assertEquals(info.getPrefix(), "");
    Assert.assertTrue(info.isCurrent());
  }

  @Test
  public void getCurrentWithPrefix() {
    StateStoreTableInfo info = StateStoreTableInfo.get("prefix-current.jst");
    Assert.assertEquals(info.getPrefix(), "prefix");
    Assert.assertTrue(info.isCurrent());
  }

  @Test
  public void getEndsWithSeparator() {
    StateStoreTableInfo info = StateStoreTableInfo.get("prefix-.jst");
    Assert.assertEquals(info.getPrefix(), "prefix");
    Assert.assertFalse(info.isCurrent());
  }
}
