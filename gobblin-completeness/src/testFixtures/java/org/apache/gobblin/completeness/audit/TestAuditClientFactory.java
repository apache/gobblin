package org.apache.gobblin.completeness.audit;

import org.apache.gobblin.configuration.State;

public class TestAuditClientFactory implements AuditCountClientFactory {
  @Override
  public AuditCountClient createAuditCountClient(State state) {
    return new TestAuditClient(state);
  }
}
