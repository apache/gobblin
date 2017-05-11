package gobblin.compaction.audit;

import gobblin.annotation.Alias;
import gobblin.configuration.State;

/**
 * Factory to create an instance of type {@link PinotAuditCountHttpClient}
 */
@Alias("PinotAuditCountHttpClientFactory")
public class PinotAuditCountHttpClientFactory implements AuditCountClientFactory {

  public PinotAuditCountHttpClient createAuditCountClient (State state) {
    return new PinotAuditCountHttpClient(state);
  }
}