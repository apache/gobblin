package gobblin.compaction.verify;

import java.io.IOException;
import java.util.Map;

/**
 * A type of client used to query the audit counts from Pinot backend
 */
public interface PinotAuditCountClient {
  Map<String, Long> fetch (String topic, long start, long end) throws IOException;
}
