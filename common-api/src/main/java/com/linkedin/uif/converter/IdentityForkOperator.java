package com.linkedin.uif.converter;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;

/**
 * An implementation of {@link ForkOperator} that simply copy the input schema
 * and data record into each forked branch. This class is useful if a converted
 * data record needs to be written to different destinations.
 *
 * @author ynli
 */
@SuppressWarnings("unused")
public class IdentityForkOperator<S, D> implements ForkOperator<S, S, D, D> {

  // Reuse both lists to save the cost of allocating new lists
  private final List<Optional<S>> schemas = Lists.newArrayList();
  private final List<Optional<D>> records = Lists.newArrayList();

  @Override
  public void init(WorkUnitState workUnitState) {
    // Do nothing
  }

  @Override
  public int getBranches(WorkUnitState workUnitState) {
    return workUnitState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
  }

  @Override
  public List<Optional<S>> forkSchema(WorkUnitState workUnitState, S input)
      throws SchemaConversionException {

    schemas.clear();
    Optional<S> copy = Optional.of(input);
    for (int i = 0; i < getBranches(workUnitState); i++) {
      schemas.add(copy);
    }

    return schemas;
  }

  @Override
  public List<Optional<D>> forkDataRecord(WorkUnitState workUnitState, D input)
      throws DataConversionException {

    records.clear();
    Optional<D> copy = Optional.of(input);
    for (int i = 0; i < getBranches(workUnitState); i++) {
      records.add(copy);
    }

    return records;
  }

  @Override
  public void close() throws IOException {
    // Nothing to do
  }
}
