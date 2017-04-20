package gobblin.compaction.verify;

import gobblin.configuration.State;

/**
 * A factory class responsible for creating {@link PinotAuditCountClient}
 */
public interface PinotAuditCountClientFactory {
  String PINOT_AUDIT_COUNT_CLIENT_FACTORY = "pinot.audit.count.client.factory";
  String DEFAULT_PINOT_AUDIT_COUNT_CLIENT_FACTORY = "PinotAuditCountHttpClientFactory";
  PinotAuditCountClient createPinotAuditCountClient (State state);
}
