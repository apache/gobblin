package gobblin.compaction.verify;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import gobblin.compaction.audit.AuditCountClient;
import gobblin.compaction.dataset.TimeBasedSubDirDatasetsFinder;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Map;

/**
 * Class to test audit count verification logic
 */
public class PinotAuditCountVerifierTest {
  public static final String PRODUCER_TIER = "producer";
  public static final String ORIGIN_TIER = "origin";
  public static final String GOBBLIN_TIER = "gobblin";


  @Test
  public void testTier() throws Exception {
    final String topic = "randomTopic";
    final String input = "/base/input";
    final String output = "/base/output";
    final String inputSub = "hourly";
    final String outputSub = "hourly";
    TestAuditCountClient client = new TestAuditCountClient();
    FileSystemDataset dataset = new FileSystemDataset() {
      @Override
      public Path datasetRoot() {
        return new Path (input + topic + inputSub + "/2017/04/03/10");
      }

      @Override
      public String datasetURN() {
        return input + topic + inputSub + "/2017/04/03/10";
      }
    };

    State props = new State();
    props.setProp (CompactionAuditCountVerifier.PRODUCER_TIER, PRODUCER_TIER);
    props.setProp (CompactionAuditCountVerifier.ORIGIN_TIER, ORIGIN_TIER);
    props.setProp (CompactionAuditCountVerifier.GOBBLIN_TIER, GOBBLIN_TIER);

    props.setProp (MRCompactor.COMPACTION_INPUT_DIR, input);
    props.setProp (MRCompactor.COMPACTION_INPUT_SUBDIR, inputSub);
    props.setProp (MRCompactor.COMPACTION_DEST_DIR, output);
    props.setProp (MRCompactor.COMPACTION_DEST_SUBDIR, outputSub);
    props.setProp (MRCompactor.COMPACTION_TMP_DEST_DIR, "/tmp/compaction/verifier");
    props.setProp (TimeBasedSubDirDatasetsFinder.COMPACTION_TIMEBASED_MAX_TIME_AGO, "3000d");
    props.setProp (TimeBasedSubDirDatasetsFinder.COMPACTION_TIMEBASED_MIN_TIME_AGO, "1d");

    CompactionAuditCountVerifier verifier = new CompactionAuditCountVerifier (props, client);

    // All complete
    client.setCounts(ImmutableMap.of(
           PRODUCER_TIER, 1000L,
           ORIGIN_TIER,   1000L,
           GOBBLIN_TIER,  1000L
    ));

    Assert.assertTrue (verifier.verify(dataset));

    // test true because GOBBLIN_TIER / PRODUCER_TIER is above threshold
    client.setCounts(ImmutableMap.of(
            PRODUCER_TIER, 1000L,
            ORIGIN_TIER,   1100L,
            GOBBLIN_TIER,  1000L
    ));
    Assert.assertTrue (verifier.verify(dataset));


    // test false because GOBBLIN_TIER / (PRODUCER_TIER || ORIGIN_TIER) is below threshold
    client.setCounts(ImmutableMap.of(
            PRODUCER_TIER, 1100L,
            ORIGIN_TIER,   1100L,
            GOBBLIN_TIER,  1000L
    ));
    Assert.assertFalse (verifier.verify(dataset));
  }


  /**
   * A helper client
   */
  public class TestAuditCountClient implements AuditCountClient {
    @Setter
    @Getter
    Map<String, Long> counts = Maps.newHashMap();

    public Map<String, Long> fetch (String topic, long start, long end) {
      return counts;
    }
  }

}
