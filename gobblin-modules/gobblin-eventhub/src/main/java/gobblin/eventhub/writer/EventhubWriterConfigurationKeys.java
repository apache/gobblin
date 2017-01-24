package gobblin.eventhub.writer;

/**
 * Configuration keys for a Eventhub.
 */


public class EventhubWriterConfigurationKeys {

    /** Writer specific configuration keys go here **/

  static final String COMMIT_TIMEOUT_MILLIS_CONFIG = "writer.eventhub.commitTimeoutMillis";
  static final long   COMMIT_TIMEOUT_MILLIS_DEFAULT = 60000; // 1 minute
  static final String COMMIT_STEP_WAIT_TIME_CONFIG = "writer.eventhub.commitStepWaitTimeMillis";
  static final long   COMMIT_STEP_WAIT_TIME_DEFAULT = 500; // 500ms
  static final String FAILURE_ALLOWANCE_PCT_CONFIG = "writer.eventhub.failureAllowancePercentage";
  static final double FAILURE_ALLOWANCE_PCT_DEFAULT = 20.0;

  static final String BATCH_TTL = "writer.eventhub.batch.ttl";
  static final long   BATCH_TTL_DEFAULT = 3000; // 3 seconds
  static final String BATCH_SIZE = "writer.eventhub.batch.size";
  static final long   BATCH_SIZE_DEFAULT = 128 * 1024; // 128KB

  public final static String  EVH_NAMESPACE = "eventhub.namespace";
  public final static String  EVH_HUBNAME = "eventhub.hubname";
  public final static String  EVH_SAS_KEYNAME = "eventhub.sas.keyname";
  public final static String  EVH_SAS_KEYVALUE = "eventhub.sas.keyvalue";
}
