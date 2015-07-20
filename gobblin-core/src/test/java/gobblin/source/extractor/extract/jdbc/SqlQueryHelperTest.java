package gobblin.source.extractor.extract.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link SqlQueryHelper}
 */
public class SqlQueryHelperTest {

  @Test
  public void testAddPredicate() {
    Assert.assertEquals(SqlQueryHelper.addPredicate("SELECT foo FROM bar", "foo != 'blah'"),
                        "SELECT foo FROM bar where (foo != 'blah')");
    Assert.assertEquals(SqlQueryHelper.addPredicate("SELECT foo,whereTo FROM bar WHERE whereTo==foo", "foo != 'blah'"),
        "SELECT foo,whereTo FROM bar WHERE whereTo==foo and (foo != 'blah')");
    Assert.assertEquals(SqlQueryHelper.addPredicate("SELECT foo,andThis FROM bar WHERE andThis>foo", "foo != 'blah'"),
        "SELECT foo,andThis FROM bar WHERE andThis>foo and (foo != 'blah')");
    try {
      SqlQueryHelper.addPredicate("SELECT foo,foo1 WHERE foo1==foo", "foo != 'blah'");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(e.toString().contains("'from'"));
    }
    try {
      SqlQueryHelper.addPredicate("SELECT foo,foo1 FROM blah WHERE foo1==foo ORDER by foo",
                                  "foo != 'blah'");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(e.toString().contains("'order by'"));
    }
    try {
      SqlQueryHelper.addPredicate("SELECT foo,foo1 FROM blah WHERE foo1==foo GROUP BY foo",
                                  "foo != 'blah'");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(e.toString().contains("'group by'"));
    }
    try {
      SqlQueryHelper.addPredicate("SELECT foo,foo1 FROM blah WHERE foo1==foo HAVING foo1 is null",
                                  "foo != 'blah'");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(e.toString().contains("'having'"));
    }
    try {
      SqlQueryHelper.addPredicate("SELECT foo,foo1 FROM blah WHERE foo1==foo LIMIT 10",
                                  "foo != 'blah'");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(e.toString().contains("'limit'"));
    }
  }

}
