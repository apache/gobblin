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
package gobblin.source.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link SqlQueryUtils}
 */
public class SqlQueryUtilsTest {

  @Test
  public void testAddPredicate() {
    Assert.assertEquals(SqlQueryUtils.addPredicate("SELECT foo FROM bar", "foo != 'blah'"),
                        "SELECT foo FROM bar where (foo != 'blah')");
    Assert.assertEquals(SqlQueryUtils.addPredicate("SELECT foo,whereTo FROM bar WHERE whereTo==foo", "foo != 'blah'"),
        "SELECT foo,whereTo FROM bar WHERE whereTo==foo and (foo != 'blah')");
    Assert.assertEquals(SqlQueryUtils.addPredicate("SELECT foo,andThis FROM bar WHERE andThis>foo", "foo != 'blah'"),
        "SELECT foo,andThis FROM bar WHERE andThis>foo and (foo != 'blah')");
    Assert.assertEquals(SqlQueryUtils.addPredicate("SELECT foo FROM bar", null),
                        "SELECT foo FROM bar");
    Assert.assertEquals(SqlQueryUtils.addPredicate("SELECT foo FROM bar", ""),
        "SELECT foo FROM bar");
    try {
      SqlQueryUtils.addPredicate("SELECT foo,foo1 WHERE foo1==foo", "foo != 'blah'");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(e.toString().contains("'from'"));
    }
    try {
      SqlQueryUtils.addPredicate("SELECT foo,foo1 FROM blah WHERE foo1==foo ORDER by foo",
                                  "foo != 'blah'");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(e.toString().contains("'order by'"));
    }
    try {
      SqlQueryUtils.addPredicate("SELECT foo,foo1 FROM blah WHERE foo1==foo GROUP BY foo",
                                  "foo != 'blah'");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(e.toString().contains("'group by'"));
    }
    try {
      SqlQueryUtils.addPredicate("SELECT foo,foo1 FROM blah WHERE foo1==foo HAVING foo1 is null",
                                  "foo != 'blah'");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(e.toString().contains("'having'"));
    }
    try {
      SqlQueryUtils.addPredicate("SELECT foo,foo1 FROM blah WHERE foo1==foo LIMIT 10",
                                  "foo != 'blah'");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(e.toString().contains("'limit'"));
    }
  }

  @Test
  public void testCastToBoolean() {
    Assert.assertTrue(SqlQueryUtils.castToBoolean("y"));
    Assert.assertTrue(SqlQueryUtils.castToBoolean("yes"));
    Assert.assertTrue(SqlQueryUtils.castToBoolean("t"));
    Assert.assertTrue(SqlQueryUtils.castToBoolean("true"));
    Assert.assertTrue(SqlQueryUtils.castToBoolean("1"));

    Assert.assertFalse(SqlQueryUtils.castToBoolean("n"));
    Assert.assertFalse(SqlQueryUtils.castToBoolean("no"));
    Assert.assertFalse(SqlQueryUtils.castToBoolean("f"));
    Assert.assertFalse(SqlQueryUtils.castToBoolean("false"));
    Assert.assertFalse(SqlQueryUtils.castToBoolean("0"));

    Assert.assertTrue(SqlQueryUtils.castToBoolean("YeS"));

    Assert.assertFalse(SqlQueryUtils.castToBoolean(""));
    Assert.assertFalse(SqlQueryUtils.castToBoolean("asdfafgsagareg"));

  }

}