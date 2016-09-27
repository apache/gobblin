package gobblin.util.request_allocation;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;



public class GreedyAllocatorTest {
  @Test
  public void testAllocateRequests()
      throws Exception {
    RequestAllocatorConfig<StringRequest> configuration =
        RequestAllocatorConfig.builder(new StringRequest.StringRequestEstimator()).build();
    GreedyAllocator<StringRequest> allocator =
        new GreedyAllocator<>(configuration);

    ResourcePool pool = ResourcePool.builder().maxResource(StringRequest.MEMORY, 100.).build();

    List<Requestor<StringRequest>> requests = Lists.<Requestor<StringRequest>>newArrayList(
        new StringRequestor("r1", "a-50", "f-50", "k-20"),
        new StringRequestor("r2", "j-10", "b-20", "e-20"),
        new StringRequestor("r3", "g-20", "c-200", "d-30"));

    AllocatedRequestsIterator<StringRequest> result = allocator.allocateRequests(requests.iterator(), pool);

    List<StringRequest> resultList = Lists.newArrayList(result);

    Assert.assertEquals(resultList.size(), 2);
    // all equal, so no order guaranteed
    Assert.assertEquals(Sets.newHashSet(Lists.transform(resultList, new Function<StringRequest, String>() {
      @Override
      public String apply(StringRequest input) {
        return input.getString();
      }
    })), Sets.newHashSet("a-50", "f-50"));
  }

}