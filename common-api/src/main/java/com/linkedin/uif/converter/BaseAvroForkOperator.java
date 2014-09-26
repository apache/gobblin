package com.linkedin.uif.converter;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;

/**
 * A base abstract class implementing the {@link ForkOperator} interface that
 * forks from an Avro schema into a list of Avro schemas and an Avro record
 * into a list of Avro records.
 *
 * @author ynli
 */
public abstract class BaseAvroForkOperator
    implements ForkOperator<Schema, Schema, GenericRecord, GenericRecord> {

  @Override
  public int getBranches(WorkUnitState workUnitState) {
    return workUnitState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY);
  }

  @Override
  public abstract List<Optional<Schema>> forkSchema(WorkUnitState workUnitState,
                                                    Schema input)
      throws SchemaConversionException;

  @Override
  public abstract List<Optional<GenericRecord>> forkDataRecord(WorkUnitState workUnitState,
                                                               GenericRecord input)
      throws DataConversionException;
}
