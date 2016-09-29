package gobblin.util.request_allocation;

import org.testng.Assert;
import org.testng.annotations.Test;


public class ResourcePoolTest {

  public static final String MEMORY = "Memory";
  public static final String TIME = "Time";

  @Test
  public void test() {

    ResourcePool pool = ResourcePool.builder().maxResource(MEMORY, 1000.).maxResource(TIME, 200.).tolerance(MEMORY, 2.)
        .defaultRequirement(TIME, 1.).build();

    Assert.assertEquals(pool.getNumDimensions(), 2);
    Assert.assertEquals(pool.getSoftBound(), new double[]{1000, 200});
    // Default tolerance is 1.2
    Assert.assertEquals(pool.getHardBound(), new double[]{2000, 240});
    // Test default resource use
    Assert.assertEquals(pool.getResourceRequirementBuilder().build().getResourceVector(), new double[]{0, 1});

    ResourceRequirement requirement = pool.getResourceRequirementBuilder().setRequirement(MEMORY, 10.).build();
    Assert.assertEquals(requirement.getResourceVector(), new double[]{10, 1});
  }

}