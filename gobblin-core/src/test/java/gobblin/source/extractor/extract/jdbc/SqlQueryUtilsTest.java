/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.source.extractor.extract.jdbc;

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

}
