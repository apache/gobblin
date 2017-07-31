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

package org.apache.gobblin.data.management.copy.hive;

import org.testng.annotations.Test;

import org.testng.Assert;


public class WhitelistBlacklistTest {

  @Test
  public void testSimpleWhitelist() throws Exception {
    WhitelistBlacklist whitelistBlacklist = new WhitelistBlacklist("Dba.Tablea", "");
    Assert.assertTrue(whitelistBlacklist.acceptDb("dba"));
    Assert.assertFalse(whitelistBlacklist.acceptDb("dbb"));

    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tablea"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("dba", "tableb"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("dbb", "tablea"));

    Assert.assertTrue(whitelistBlacklist.acceptTable("dbA", "tablea"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dbA", "TableA"));
  }

  @Test
  public void testSimpleBlacklist() throws Exception {
    WhitelistBlacklist whitelistBlacklist = new WhitelistBlacklist("", "dba.tablea");
    Assert.assertTrue(whitelistBlacklist.acceptDb("dba"));
    Assert.assertTrue(whitelistBlacklist.acceptDb("dbb"));

    Assert.assertFalse(whitelistBlacklist.acceptTable("dba", "tablea"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tableb"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dbb", "tablea"));
  }

  @Test
  public void testDbWhitelist() throws Exception {
    WhitelistBlacklist whitelistBlacklist = new WhitelistBlacklist("dba", "");
    Assert.assertTrue(whitelistBlacklist.acceptDb("dba"));
    Assert.assertFalse(whitelistBlacklist.acceptDb("dbb"));

    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tablea"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tableb"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("dbb", "tablea"));
  }

  @Test
  public void testDbBlacklist() throws Exception {
    WhitelistBlacklist whitelistBlacklist = new WhitelistBlacklist("", "dba");
    Assert.assertFalse(whitelistBlacklist.acceptDb("dba"));
    Assert.assertTrue(whitelistBlacklist.acceptDb("dbb"));

    Assert.assertFalse(whitelistBlacklist.acceptTable("dba", "tablea"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("dba", "tableb"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dbb", "tablea"));
  }

  @Test
  public void testDbWhitelistStar() throws Exception {
    WhitelistBlacklist whitelistBlacklist = new WhitelistBlacklist("dba.*", "");
    Assert.assertTrue(whitelistBlacklist.acceptDb("dba"));
    Assert.assertFalse(whitelistBlacklist.acceptDb("dbb"));

    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tablea"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tableb"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("dbb", "tablea"));
  }

  @Test
  public void testDbBlacklistStar() throws Exception {
    WhitelistBlacklist whitelistBlacklist = new WhitelistBlacklist("", "dba.*");
    Assert.assertFalse(whitelistBlacklist.acceptDb("dba"));
    Assert.assertTrue(whitelistBlacklist.acceptDb("dbb"));

    Assert.assertFalse(whitelistBlacklist.acceptTable("dba", "tablea"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("dba", "tableb"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dbb", "tablea"));
  }

  @Test
  public void testMultiWhitelist() throws Exception {
    WhitelistBlacklist whitelistBlacklist = new WhitelistBlacklist("dba,dbb.tablea", "");
    Assert.assertTrue(whitelistBlacklist.acceptDb("dba"));
    Assert.assertTrue(whitelistBlacklist.acceptDb("dbb"));
    Assert.assertFalse(whitelistBlacklist.acceptDb("dbc"));

    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tablea"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tableb"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dbb", "tablea"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("dbc", "tablea"));
  }

  @Test
  public void testMultipleTablesWhitelist() throws Exception {
    WhitelistBlacklist whitelistBlacklist = new WhitelistBlacklist("dba.tablea|tableb", "");
    Assert.assertTrue(whitelistBlacklist.acceptDb("dba"));
    Assert.assertFalse(whitelistBlacklist.acceptDb("dbb"));

    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tablea"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tableb"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("dba", "tablec"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("dbb", "tablea"));
  }

  @Test
  public void testTablePattern() throws Exception {
    WhitelistBlacklist whitelistBlacklist = new WhitelistBlacklist("dba.table*|accept", "");
    Assert.assertTrue(whitelistBlacklist.acceptDb("dba"));
    Assert.assertFalse(whitelistBlacklist.acceptDb("dbb"));

    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tablea"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tableb"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "accept"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("dba", "other"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("dbb", "tablea"));
  }

  @Test
  public void testDbPattern() throws Exception {
    WhitelistBlacklist whitelistBlacklist = new WhitelistBlacklist("db*", "");
    Assert.assertTrue(whitelistBlacklist.acceptDb("dba"));
    Assert.assertTrue(whitelistBlacklist.acceptDb("dbb"));
    Assert.assertFalse(whitelistBlacklist.acceptDb("database"));

    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tablea"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tableb"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dbb", "tablea"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("database", "tablea"));
  }

  @Test
  public void testDbAndTablePattern() throws Exception {
    WhitelistBlacklist whitelistBlacklist = new WhitelistBlacklist("db*.table*", "");
    Assert.assertTrue(whitelistBlacklist.acceptDb("dba"));
    Assert.assertTrue(whitelistBlacklist.acceptDb("dbb"));
    Assert.assertFalse(whitelistBlacklist.acceptDb("database"));

    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tablea"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tableb"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("dba", "other"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dbb", "tablea"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("database", "tablea"));
  }

  @Test
  public void testWhitelistBlacklist() throws Exception {
    WhitelistBlacklist whitelistBlacklist = new WhitelistBlacklist("dba", "dba.tablea");
    Assert.assertTrue(whitelistBlacklist.acceptDb("dba"));
    Assert.assertFalse(whitelistBlacklist.acceptDb("dbb"));

    Assert.assertFalse(whitelistBlacklist.acceptTable("dba", "tablea"));
    Assert.assertTrue(whitelistBlacklist.acceptTable("dba", "tableb"));
    Assert.assertFalse(whitelistBlacklist.acceptTable("dbb", "tableb"));

  }

}
