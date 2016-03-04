/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.hive;

import java.io.IOException;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;
import gobblin.hive.avro.HiveAvroSerDeManager;
import lombok.extern.slf4j.Slf4j;


/**
 * This class manages SerDe properties (including schema properties) for Hive registration.
 *
 * @author ziliu
 */
@Slf4j
@Alpha
public abstract class HiveSerDeManager {

  public static final String HIVE_ROW_FORMAT = "hive.row.format";

  protected final State props;

  protected HiveSerDeManager(State props) {
    this.props = props;
  }

  /**
   * Add the appropriate SerDe properties (including schema properties) to the given {@link HiveRegistrationUnit}.
   *
   * @param path The {@link Path} from where the schema should be obtained.
   * @param hiveUnit The {@link HiveRegistrationUnit} where the serde properties should be added to.
   * @throws IOException
   */
  public abstract void addSerDeProperties(Path path, HiveRegistrationUnit hiveUnit) throws IOException;

  /**
   * Update the schema in the existing {@link HiveRegistrationUnit} into the schema in the new
   * {@link HiveRegistrationUnit}.
   */
  public abstract void updateSchema(HiveRegistrationUnit existingUnit, HiveRegistrationUnit newUnit) throws IOException;

  public enum Implementation {
    AVRO(HiveAvroSerDeManager.class.getName());

    private final String schemaManagerClassName;

    private Implementation(String schemaManagerClassName) {
      this.schemaManagerClassName = schemaManagerClassName;
    }

    @Override
    public String toString() {
      return this.schemaManagerClassName;
    }
  }

  /**
   * Get an instance of {@link HiveSerDeManager}.
   *
   * @param type The {@link HiveSerDeManager} type. It should be either AVRO, or the name of a class that implements
   * {@link HiveSerDeManager}. The specified {@link HiveSerDeManager} type must have a constructor that takes a
   * {@link State} object.
   * @param props A {@link State} object. To get a specific implementation of {@link HiveSerDeManager}, specify either
   * one of the values in {@link Implementation} (e.g., AVRO) or the name of a class that implements
   * {@link HiveSerDeManager} in property {@link #HIVE_ROW_FORMAT}. The {@link State} object is also used to
   * instantiate the {@link HiveSerDeManager}.
   */
  public static HiveSerDeManager get(State props) {
    String type = props.getProp(HIVE_ROW_FORMAT, Implementation.AVRO.name());
    Optional<Implementation> implementation = Enums.getIfPresent(Implementation.class, type.toUpperCase());
    if (implementation.isPresent()) {
      try {
        return (HiveSerDeManager) ConstructorUtils.invokeConstructor(Class.forName(implementation.get().toString()),
            props);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(
            "Unable to instantiate " + HiveSerDeManager.class.getSimpleName() + " with type " + type, e);
      }
    } else {
      log.info(String.format("%s with type %s does not exist. Using %s", HiveSerDeManager.class.getSimpleName(), type,
          HiveAvroSerDeManager.class.getSimpleName()));
      try {
        return new HiveAvroSerDeManager(props);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

}
