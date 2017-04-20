package gobblin.compaction.verify;

import com.google.common.base.Splitter;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.compaction.parser.CompactionPathParser;
import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;
import gobblin.util.ClassAliasResolver;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

/**
 * Pinot based audit count verifier
 * Use {@link PinotAuditCountClient} to retrieve all record count across different tiers
 * Compare one specific tier (gobblin-tier) with all other refernce tiers and determine
 * if verification should be passed based on a pre-defined threshold.
 */
@Slf4j
public class PinotAuditCompletenessVerifier extends CompactionAuditCountVerifier {

  public static final String COMPACTION_COMPLETENESS_THRESHOLD = MRCompactor.COMPACTION_PREFIX + "completeness.threshold";
  public static final double DEFAULT_COMPACTION_COMPLETENESS_THRESHOLD = 0.99;
  public static final String PRODUCER_TIER = "producer.tier";
  public static final String ORIGIN_TIER = "origin.tier";
  public static final String GOBBLIN_TIER = "gobblin.tier";

  private final Collection<String> referenceTiers;
  private final Collection<String> originTiers;
  private final String producerTier;
  private final String gobblinTier;
  private final double threshold;
  private final PinotAuditCountClient auditCountClient;

  /**
   * Constructor with default audit count client
   */
  public PinotAuditCompletenessVerifier (State state) throws Exception {
    this (state, getClientFactory (state).createPinotAuditCountClient(state));
  }

  /**
   * Constructor with user specified audit count client
   */
  public PinotAuditCompletenessVerifier (State state, PinotAuditCountClient client) {
    super (state);
    this.auditCountClient = client;
    this.threshold =
            state.getPropAsDouble(COMPACTION_COMPLETENESS_THRESHOLD, DEFAULT_COMPACTION_COMPLETENESS_THRESHOLD);

    // retrieve all tiers information
    this.producerTier = state.getProp(PRODUCER_TIER);
    this.gobblinTier = state.getProp(GOBBLIN_TIER);
    this.originTiers = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(state.getProp(ORIGIN_TIER));
    this.referenceTiers = new HashSet<>(originTiers);
    this.referenceTiers.add(producerTier);
  }

  private static PinotAuditCountClientFactory getClientFactory (State state) {
    try {
      String factoryName = state.getProp(PinotAuditCountClientFactory.PINOT_AUDIT_COUNT_CLIENT_FACTORY,
              PinotAuditCountClientFactory.DEFAULT_PINOT_AUDIT_COUNT_CLIENT_FACTORY);

      ClassAliasResolver<PinotAuditCountClientFactory> conditionClassAliasResolver = new ClassAliasResolver<>(PinotAuditCountClientFactory.class);
      PinotAuditCountClientFactory factory = conditionClassAliasResolver.resolveClass(factoryName).newInstance();
      return factory;
    } catch (Exception e) {
      log.error(e.toString());
    }
    return null;
  }

  /**
   * Verify a specific dataset by following below steps
   *    1) Retrieve a tier-to-count mapping
   *    2) Read count from {@link PinotAuditCompletenessVerifier#gobblinTier}
   *    3) Read count from all other {@link PinotAuditCompletenessVerifier#referenceTiers}
   *    4) Compare count retrieved from steps 2) and 3), if any of (gobblin/refenence) >= threshold, return true, else return false
   * @param dataset Dataset needs to be verified
   * @return If verification is succeeded
   */
  public boolean verify (FileSystemDataset dataset) {
    CompactionPathParser.CompactionParserResult result = new CompactionPathParser(this.state).parse(dataset);
    DateTime startTime = result.getTime();
    DateTime endTime = startTime.plusHours(1);
    String datasetName = result.getDatasetName();
    try {
      Map<String, Long> countsByTier = auditCountClient.fetch (datasetName, startTime.getMillis(), endTime.getMillis());
      for (String tier: referenceTiers) {
        if (passed (datasetName, countsByTier, tier)) {
          return true;
        }
      }
    } catch (IOException e) {
      log.error(e.toString());
    }

    return false;
  }

  /**
   * Compare record count between {@link PinotAuditCompletenessVerifier#gobblinTier} and {@link PinotAuditCompletenessVerifier#referenceTiers}.
   * @param datasetName the name of dataset
   * @param countsByTier the tier-to-count mapping retrieved by {@link PinotAuditCountClient#fetch(String, long, long)}
   * @param referenceTier the tiers we wants to compare against
   * @return If any of (gobblin/refenence) >= threshold, return true, else return false
   */
  private boolean passed (String datasetName, Map<String, Long> countsByTier, String referenceTier) {
    if (!countsByTier.containsKey(this.gobblinTier)) {
      log.warn(String
              .format("Failed to get audit count for topic %s, tier %s", datasetName, this.gobblinTier));
      return false;
    }
    if (!countsByTier.containsKey(referenceTier)) {
      log.warn(String.format("Failed to get audit count for topic %s, tier %s", datasetName, referenceTier));
      return false;
    }

    long originCount = countsByTier.get(referenceTier);
    long gobblinCount = countsByTier.get(this.gobblinTier);

    if ((double) gobblinCount / (double) originCount < this.threshold) {
      log.warn(String.format("Verification failed for %s : gobblin count = %d, originCount count = %d (%f)",
              datasetName, gobblinCount, originCount, (double) gobblinCount / (double) originCount));
      return false;
    }

    return true;
  }
}
