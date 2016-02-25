package gobblin.util.io;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.gson.Gson;

import gobblin.util.test.BaseClass;
import gobblin.util.test.TestClass;


public class GsonInterfaceAdapterTest {

  @Test
  public void test() {
    Gson gson = GsonInterfaceAdapter.getGson(Object.class);

    TestClass test = new TestClass();
    test.absent = Optional.absent();
    Assert.assertNotEquals(test, new TestClass());

    String ser = gson.toJson(test);
    BaseClass deser = gson.fromJson(ser, BaseClass.class);
    Assert.assertEquals(test, deser);

  }

}
