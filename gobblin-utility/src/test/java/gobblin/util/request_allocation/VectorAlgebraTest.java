package gobblin.util.request_allocation;

import org.testng.Assert;
import org.testng.annotations.Test;


public class VectorAlgebraTest {
  @Test
  public void testAddVector()
      throws Exception {
    Assert.assertEquals(VectorAlgebra.addVector(new double[]{1, 2}, new double[]{1, 3}, 1., null), new double[]{2, 5});
    Assert.assertEquals(VectorAlgebra.addVector(new double[]{1, 2}, new double[]{1, 3}, 2., null), new double[]{3, 8});
    Assert.assertEquals(VectorAlgebra.addVector(new double[]{1, 2}, new double[]{1, 3}, -1., null), new double[]{0, -1});

    // Check it uses reuse vector
    double[] reuse = new double[]{1, 2};
    VectorAlgebra.addVector(reuse, new double[]{1, 3}, 1., reuse);
    Assert.assertEquals(reuse, new double[]{2, 5});
  }

  @Test
  public void testExceedsVector()
      throws Exception {
    Assert.assertTrue(VectorAlgebra.exceedsVector(new double[]{1, 2}, new double[]{0, 3}, false));
    Assert.assertTrue(VectorAlgebra.exceedsVector(new double[]{1, 2}, new double[]{2, 0}, false));
    Assert.assertTrue(VectorAlgebra.exceedsVector(new double[]{1, 2}, new double[]{0, 2}, true));
    Assert.assertTrue(VectorAlgebra.exceedsVector(new double[]{1, 2}, new double[]{1, 0}, true));

    Assert.assertFalse(VectorAlgebra.exceedsVector(new double[]{1, 2}, new double[]{0, 1}, false));
    Assert.assertFalse(VectorAlgebra.exceedsVector(new double[]{1, 2}, new double[]{1, 2}, false));
  }
}