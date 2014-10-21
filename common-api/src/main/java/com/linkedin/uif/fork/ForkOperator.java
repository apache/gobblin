package com.linkedin.uif.fork;

import java.io.Closeable;
import java.util.List;

import com.linkedin.uif.configuration.WorkUnitState;

/**
 * An interface for fork operators that convert one input data record into multiple
 * records. So essentially this operator forks one input data stream into multiple
 * data streams. This interface allows user to plugin their fork logic.
 *
 * @author ynli
 *
 * @param <S> schema data type
 * @param <D> data record data type
 */
public interface ForkOperator<S, D> extends Closeable {

  /**
   * Initialize this {@link ForkOperator}.
   *
   * @param workUnitState {@link WorkUnitState} carrying the configuration
   */
  public void init(WorkUnitState workUnitState) throws Exception;

  /**
   * Get the number of branches after the fork.
   *
   * @param workUnitState {@link WorkUnitState} carrying the configuration
   * @return number of branches after the fork
   */
  public int getBranches(WorkUnitState workUnitState);

  /**
   * Get a list of {@link java.lang.Boolean}s indicating if the schema should go to each branch.
   *
   * @param workUnitState {@link WorkUnitState} carrying the configuration
   * @param input input schema
   * @return list of {@link java.lang.Boolean}s
   */
  public List<Boolean> forkSchema(WorkUnitState workUnitState, S input);

  /**
   * Get a list of {@link java.lang.Boolean}s indicating if the record should go to each branch.
   *
   * @param workUnitState {@link WorkUnitState} carrying the configuration
   * @param input input data record
   * @return list of {@link java.lang.Boolean}s
   */
  public List<Boolean> forkDataRecord(WorkUnitState workUnitState, D input);
}
