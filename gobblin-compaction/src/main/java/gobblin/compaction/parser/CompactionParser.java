package gobblin.compaction.parser;

import gobblin.configuration.State;
import gobblin.dataset.Dataset;
import lombok.AllArgsConstructor;

/**
 * A parser class which can convert a given {@link Dataset} to any object defined by user
 */
@AllArgsConstructor
public abstract class CompactionParser<D extends Dataset> {
  protected final State state;

  public abstract Object parse (D dataset);
}
