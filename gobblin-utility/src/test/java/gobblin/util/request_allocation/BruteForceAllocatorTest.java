package gobblin.util.request_allocation;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;


public class BruteForceAllocatorTest {
  @Test
  public void testAllocateRequests()
      throws Exception {
    RequestAllocatorConfig<StringRequest> configuration =
        RequestAllocatorConfig.builder(new StringRequest.StringRequestEstimator())
            .withPrioritizer(new StringRequest.StringRequestComparator()).build();
    BruteForceAllocator<StringRequest> allocator =
        new BruteForceAllocator<>(configuration);

    ResourcePool pool = ResourcePool.builder().maxResource(StringRequest.MEMORY, 100.).build();

    List<Requestor<StringRequest>> requests = Lists.<Requestor<StringRequest>>newArrayList(
        new StringRequestor("r1", "a-50", "f-50", "k-20"),
        new StringRequestor("r2", "j-10", "b-20", "e-20"),
        new StringRequestor("r3", "g-20", "c-200", "d-30"));

    AllocatedRequestsIterator<StringRequest> result = allocator.allocateRequests(requests.iterator(), pool);

    List<StringRequest> resultList = Lists.newArrayList(result);

    Assert.assertEquals(resultList.size(), 4);
    Assert.assertEquals(resultList.get(0).getString(), "a-50");
    Assert.assertEquals(resultList.get(1).getString(), "b-20");
    // No c because it is too large to fit
    Assert.assertEquals(resultList.get(2).getString(), "d-30");
    Assert.assertEquals(resultList.get(3).getString(), "e-20");
  }
}