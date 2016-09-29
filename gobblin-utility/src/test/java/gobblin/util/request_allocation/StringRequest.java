package gobblin.util.request_allocation;

import java.util.Comparator;
import java.util.Set;

import com.google.common.collect.Sets;

import lombok.Data;
import lombok.Getter;


@Data
public class StringRequest implements Request<StringRequest> {
  public static final String MEMORY = "memory";

  private final Requestor<StringRequest> requestor;
  private final String string;

  public static class StringRequestComparator implements Comparator<StringRequest> {
    @Override
    public int compare(StringRequest o1, StringRequest o2) {
      String o1CompareToken = o1.getString().split("-")[0];
      String o2CompareToken = o2.getString().split("-")[0];
      return o1CompareToken.compareTo(o2CompareToken);
    }
  }

  public static class StringRequestEstimator implements ResourceEstimator<StringRequest> {
    @Getter
    private Set<String> queriedRequests = Sets.newConcurrentHashSet();

    @Override
    public ResourceRequirement estimateRequirement(StringRequest s, ResourcePool resourcePool) {
      double memory = Double.parseDouble(s.getString().split("-")[1]);
      this.queriedRequests.add(s.getString());
      return resourcePool.getResourceRequirementBuilder().setRequirement(MEMORY, memory).build();
    }
  }
}
