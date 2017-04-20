package gobblin.compaction.verify;

import gobblin.annotation.Alias;
import gobblin.configuration.State;

/**
 * Factory to create an instance of type {@link PinotAuditCountHttpClient}
 */
@Alias("PinotAuditCountHttpClientFactory")
public class PinotAuditCountHttpClientFactory implements PinotAuditCountClientFactory {

  public PinotAuditCountClient createPinotAuditCountClient (State state) {
    return new PinotAuditCountHttpClient (state);
  }
}