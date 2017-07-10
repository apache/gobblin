package gobblin.compaction.action;

import gobblin.compaction.event.CompactionSlaEventHelper;
import gobblin.compaction.parser.CompactionPathParser;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.dataset.FileSystemDataset;
import gobblin.hive.HiveRegister;
import gobblin.hive.policy.HiveRegistrationPolicy;
import gobblin.hive.policy.HiveRegistrationPolicyBase;
import gobblin.hive.spec.HiveSpec;
import gobblin.metrics.event.EventSubmitter;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;


/**
 * Class responsible for hive registration after compaction is complete
 */
@Slf4j
public class CompactionHiveRegistrationAction implements CompactionCompleteAction<FileSystemDataset> {
  private final State state;
  private EventSubmitter eventSubmitter;
  public CompactionHiveRegistrationAction (State state) {
    if (!(state instanceof WorkUnitState)) {
      throw new UnsupportedOperationException(this.getClass().getName() + " only supports workunit state");
    }
    this.state = state;
  }

  public void onCompactionJobComplete(FileSystemDataset dataset) throws IOException {
    if (state.contains(ConfigurationKeys.HIVE_REGISTRATION_POLICY)) {
      HiveRegister hiveRegister = HiveRegister.get(state);
      HiveRegistrationPolicy hiveRegistrationPolicy = HiveRegistrationPolicyBase.getPolicy(state);
      CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);

      List<String> paths = new ArrayList<>();
      for (HiveSpec spec : hiveRegistrationPolicy.getHiveSpecs(new Path(result.getDstAbsoluteDir()))) {
        hiveRegister.register(spec);
        paths.add(spec.getPath().toUri().toASCIIString());
        log.info("Hive registration is done for {}", result.getDstAbsoluteDir());
      }

      // submit events for hive registration
      if (eventSubmitter != null) {
        Map<String, String> eventMetadataMap = new HashMap<>();
        eventMetadataMap.put("datasetUrn", dataset.datasetURN());
        eventMetadataMap.put(CompactionSlaEventHelper.HIVE_REGISTRATION_PATHS, Joiner.on(',').join(paths));
        this.eventSubmitter.submit(CompactionSlaEventHelper.COMPACTION_HIVE_REGISTRATION_EVENT, eventMetadataMap);
      }
    }
  }

  public void addEventSubmitter(EventSubmitter submitter) {
    this.eventSubmitter = submitter;
  }
}
