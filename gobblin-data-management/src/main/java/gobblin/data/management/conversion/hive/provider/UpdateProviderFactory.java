/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gobblin.data.management.conversion.hive.provider;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.collect.ImmutableList;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;
import gobblin.util.HadoopUtils;
import gobblin.util.reflection.GobblinConstructorUtils;

/**
 * A factory class to create {@link HiveUnitUpdateProvider}s
 */
@Alpha
public class UpdateProviderFactory {

  private static final String OPTIONAL_HIVE_UNIT_UPDATE_PROVIDER_CLASS_KEY = "hive.unit.updateProvider.class";
  private static final String DEFAULT_HIVE_UNIT_UPDATE_PROVIDER_CLASS = HdfsBasedUpdateProvider.class
      .getName();

  public static HiveUnitUpdateProvider create(State state) {
    try {
      return (HiveUnitUpdateProvider) GobblinConstructorUtils.invokeFirstConstructor(Class.forName(state.getProp(
          OPTIONAL_HIVE_UNIT_UPDATE_PROVIDER_CLASS_KEY, DEFAULT_HIVE_UNIT_UPDATE_PROVIDER_CLASS)),
          ImmutableList.<Object>of(FileSystem.get(HadoopUtils.getConfFromState(state))), ImmutableList.of());
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static HiveUnitUpdateProvider create(Properties properties) {
    try {
      return (HiveUnitUpdateProvider) GobblinConstructorUtils.invokeFirstConstructor(Class.forName(properties
              .getProperty(
              OPTIONAL_HIVE_UNIT_UPDATE_PROVIDER_CLASS_KEY, DEFAULT_HIVE_UNIT_UPDATE_PROVIDER_CLASS)),
          ImmutableList.<Object>of(FileSystem.get(HadoopUtils.getConfFromProperties(properties))), ImmutableList.of());
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException | IOException e) {
      throw new RuntimeException(e);
    }
  }
}
