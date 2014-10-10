package com.linkedin.uif.converter;

import java.io.Closeable;
import java.util.List;

import com.google.common.base.Optional;

import com.linkedin.uif.configuration.WorkUnitState;

/**
 * An interface for fork operators that convert one input data record into multiple
 * records. So essentially this operator forks one input data stream into multiple
 * data streams. This interface allows user to plugin their fork logic.
 *
 * @author ynli
 *
 * @param <SI> input schema type
 * @param <SO> output schema type
 * @param <DI> input data record type
 * @param <DO> output data record type
 */
public interface ForkOperator<SI, SO, DI, DO> extends Closeable {

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
   * Get a list of output schemas for the output data records, one for each record.
   *
   * <p>
   *   The output schemas are wrapped into {@link com.google.common.base.Optional}s,
   *   so if an input schema is not to be forked into all branches,
   *   {@link com.google.common.base.Optional#absent()} can be used to indicate that.
   * </p>
   *
   * @param workUnitState {@link WorkUnitState} carrying the configuration
   * @param input input schema
   * @return list of output schemas for the output data records
   * @throws SchemaConversionException if anything is wrong with creating the output schemas
   */
  public List<Optional<SO>> forkSchema(WorkUnitState workUnitState, SI input)
      throws SchemaConversionException;

  /**
   * Fork an input data record into a list of output data records.
   *
   * <p>
   *   The output records are wrapped into {@link com.google.common.base.Optional}s,
   *   so if an input record is not to be forked into all branches,
   *   {@link com.google.common.base.Optional#absent()} can be used to indicate that.
   * </p>
   *
   * @param workUnitState {@link WorkUnitState} carrying the configuration
   * @param input input data record
   * @return list of output data records
   * @throws DataConversionException if anything is wrong with creating the output records
   */
  public List<Optional<DO>> forkDataRecord(WorkUnitState workUnitState, DI input)
      throws DataConversionException;
}
