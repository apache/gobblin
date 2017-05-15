package gobblin.compaction.action;

import gobblin.compaction.parser.CompactionPathParser;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;
import gobblin.hive.HiveRegister;
import gobblin.hive.policy.HiveRegistrationPolicy;
import gobblin.hive.policy.HiveRegistrationPolicyBase;
import gobblin.hive.spec.HiveSpec;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Class responsible for hive registration after compaction is complete
 */
@Slf4j
public class CompactionHiveRegistrationAction implements CompactionCompleteAction<FileSystemDataset> {
  private final State state;

  public CompactionHiveRegistrationAction (State state) {
    this.state = state;
  }

  public void onCompactionJobComplete(FileSystemDataset dataset) throws IOException {
    if (state.contains(ConfigurationKeys.HIVE_REGISTRATION_POLICY)) {
      HiveRegister hiveRegister = HiveRegister.get(state);
      HiveRegistrationPolicy hiveRegistrationPolicy = HiveRegistrationPolicyBase.getPolicy(state);
      CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);

      for (HiveSpec spec : hiveRegistrationPolicy.getHiveSpecs(new Path(result.getDstAbsoluteDir()))) {
        hiveRegister.register(spec);
        log.info("Hive registration is done for {}", result.getDstAbsoluteDir());
      }
    }
  }
}
