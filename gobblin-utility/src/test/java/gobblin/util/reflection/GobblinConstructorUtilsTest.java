package gobblin.util.reflection;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class GobblinConstructorUtilsTest {

  @Test
  public void testInvokeFirst() throws Exception {

    ConstructorTestClass ctc = GobblinConstructorUtils.invokeFirstConstructor(ConstructorTestClass.class,
        ImmutableList.<Object> of(Integer.valueOf(3), new Properties()), ImmutableList.<Object> of(Integer.valueOf(3), "test1"));

    Assert.assertNotNull(ctc.id);
    Assert.assertNotNull(ctc.props);
    Assert.assertEquals(ctc.id, Integer.valueOf(3));

    Assert.assertNull(ctc.str);

    ctc = GobblinConstructorUtils.invokeFirstConstructor(ConstructorTestClass.class,
        ImmutableList.<Object> of(Integer.valueOf(3), "test1"), ImmutableList.<Object> of(Integer.valueOf(3), new Properties()));

    Assert.assertNotNull(ctc.id);
    Assert.assertNotNull(ctc.str);
    Assert.assertEquals(ctc.id, Integer.valueOf(3));
    Assert.assertEquals(ctc.str, "test1");

    Assert.assertNull(ctc.props);
  }

  @Test(expectedExceptions = NoSuchMethodException.class)
  public void testInvokeFirstException() throws Exception {
    GobblinConstructorUtils.invokeFirstConstructor(ConstructorTestClass.class,
        ImmutableList.<Object> of(), ImmutableList.<Object> of(Integer.valueOf(3), Integer.valueOf(3)));
  }

  public static class ConstructorTestClass {
    Properties props = null;
    String str = null;
    Integer id = null;
    public ConstructorTestClass(Integer id, Properties props) {
      this.id = id;
      this.props = props;
    }

    public ConstructorTestClass(Integer id, String str) {
      this.id = id;
      this.str = str;
    }
  }

  @Test
  public void testLongestConstructor() throws Exception {

    ConstructorTestClass obj =
        GobblinConstructorUtils.invokeLongestConstructor(ConstructorTestClass.class, Integer.valueOf(1), "String1", "String2");
    Assert.assertEquals(obj.id.intValue(), 1);
    Assert.assertEquals(obj.str, "String1");

    obj =
        GobblinConstructorUtils.invokeLongestConstructor(ConstructorTestClass.class, Integer.valueOf(1), "String1");
    Assert.assertEquals(obj.id.intValue(), 1);
    Assert.assertEquals(obj.str, "String1");

    try {
      obj =
          GobblinConstructorUtils.invokeLongestConstructor(ConstructorTestClass.class, Integer.valueOf(1));
      Assert.fail();
    } catch (NoSuchMethodException nsme) {
      //expected to throw exception
    }
  }
}
