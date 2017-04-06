package gobblin.ingestion.google.adwords;

import gobblin.configuration.State;
import gobblin.ingestion.google.DayPartitioner;


public class GoogleAdWordsDayPartitioner extends DayPartitioner {

  public GoogleAdWordsDayPartitioner(State state, int numBranches, int branchId) {
    super(state, numBranches, branchId);
  }
}
