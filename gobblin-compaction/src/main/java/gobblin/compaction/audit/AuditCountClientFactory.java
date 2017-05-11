package gobblin.compaction.audit;

import gobblin.configuration.State;

/**
 * A factory class responsible for creating {@link AuditCountClient}
 */
public interface AuditCountClientFactory {
  String AUDIT_COUNT_CLIENT_FACTORY = "audit.count.client.factory";
  AuditCountClient createAuditCountClient (State state);
}
