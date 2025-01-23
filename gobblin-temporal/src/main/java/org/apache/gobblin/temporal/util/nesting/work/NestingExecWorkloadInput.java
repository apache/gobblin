package org.apache.gobblin.temporal.util.nesting.work;

import java.util.Optional;
import java.util.Properties;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;


@Data
@Setter(AccessLevel.NONE) // NOTE: non-`final` members solely to enable deserialization
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@RequiredArgsConstructor
@AllArgsConstructor
public class NestingExecWorkloadInput<WORK_ITEM> {
  private WorkflowAddr addr;
  private Workload<WORK_ITEM> workload;
  private int startIndex;
  private int maxBranchesPerTree;
  private int maxSubTreesPerTree;
  private Optional<Integer> maxSubTreesForCurrentTreeOverride;
  private Properties props;
}
