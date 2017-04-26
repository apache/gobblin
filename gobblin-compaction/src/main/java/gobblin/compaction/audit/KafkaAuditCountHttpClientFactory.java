package gobblin.compaction.audit;

import gobblin.annotation.Alias;
import gobblin.configuration.State;

/**
 * Factory to create an instance of type {@link KafkaAuditCountHttpClient}
 */
@Alias("KafkaAuditCountHttpClientFactory")
public class KafkaAuditCountHttpClientFactory implements AuditCountClientFactory {

  public KafkaAuditCountHttpClient createAuditCountClient (State state)  {
    return new KafkaAuditCountHttpClient(state);
  }
}
