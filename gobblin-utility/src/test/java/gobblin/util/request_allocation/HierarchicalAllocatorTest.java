package gobblin.util.request_allocation;

import java.util.Comparator;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;


public class HierarchicalAllocatorTest {
  @Test
  public void testAllocateRequests()
      throws Exception {

    Comparator<Requestor<StringRequest>> requestorComparator = new Comparator<Requestor<StringRequest>>() {
      @Override
      public int compare(Requestor<StringRequest> o1, Requestor<StringRequest> o2) {
        StringRequestor stringRequestor1 = (StringRequestor) o1;
        StringRequestor stringRequestor2 = (StringRequestor) o2;
        return stringRequestor1.getName().compareTo(stringRequestor2.getName());
      }
    };

    HierarchicalPrioritizer<StringRequest> prioritizer =
        new SimpleHierarchicalPrioritizer<>(requestorComparator, new StringRequest.StringRequestComparator());
    RequestAllocatorConfig<StringRequest> configuration =
        RequestAllocatorConfig.builder(new StringRequest.StringRequestEstimator()).withPrioritizer(prioritizer).build();
    BruteForceAllocator<StringRequest> underlying = new BruteForceAllocator<>(configuration);
    HierarchicalAllocator<StringRequest> hierarchicalAllocator = new HierarchicalAllocator<>(prioritizer, underlying);

    List<Requestor<StringRequest>> requests = Lists.<Requestor<StringRequest>>newArrayList(
        new StringRequestor("r2", "b-10", "c-10"),
        new StringRequestor("r1", "f-10", "h-10"),
        new StringRequestor("r1", "g-10", "i-10"),
        new StringRequestor("r3", "a-10", "d-10"));
    ResourcePool pool = ResourcePool.builder().maxResource(StringRequest.MEMORY, 45.).build();

    AllocatedRequestsIterator<StringRequest> result = hierarchicalAllocator.allocateRequests(requests.iterator(), pool);
    List<StringRequest> resultList = Lists.newArrayList(result);

    Assert.assertEquals(resultList.size(), 5);
    Assert.assertEquals(resultList.get(0).getString(), "f-10");
    Assert.assertEquals(resultList.get(1).getString(), "g-10");
    Assert.assertEquals(resultList.get(2).getString(), "h-10");
    Assert.assertEquals(resultList.get(3).getString(), "i-10");
    Assert.assertEquals(resultList.get(4).getString(), "b-10");

  }
}