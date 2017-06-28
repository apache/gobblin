package gobblin.compaction.suite;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import gobblin.compaction.action.CompactionCompleteAction;
import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;

@Slf4j
public class TestCompactionSuites {

  /**
   * Test hive registration failure
   */
  public static class HiveRegistrationCompactionSuite extends CompactionAvroSuite {

    public HiveRegistrationCompactionSuite(State state) {
      super(state);
    }

    public List<CompactionCompleteAction<FileSystemDataset>> getCompactionCompleteActions() {
      ArrayList<CompactionCompleteAction<FileSystemDataset>> array = new ArrayList<>();
      array.add((dataset) -> {
        if (dataset.datasetURN().contains(TestCompactionSuiteFactories.DATASET_FAIL))
          throw new RuntimeException("test-hive-registration-failure");
      });
      return array;
    }
  }
}
