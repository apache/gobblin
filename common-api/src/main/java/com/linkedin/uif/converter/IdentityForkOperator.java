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

  @Override
  public int getBranches(WorkUnitState workUnitState) {
    return workUnitState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
  }

  @Override
  public List<Optional<S>> forkSchema(WorkUnitState workUnitState, S input)
      throws SchemaConversionException {

    Optional<S> copy = Optional.of(input);
    List<Optional<S>> schemas = Lists.newArrayList();
    for (int i = 0; i < getBranches(workUnitState); i++) {
      schemas.add(copy);
    }

    return schemas;
  }

  @Override
  public List<Optional<D>> forkDataRecord(WorkUnitState workUnitState, D input)
      throws DataConversionException {

    Optional<D> copy = Optional.of(input);
    List<Optional<D>> records = Lists.newArrayList();
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
