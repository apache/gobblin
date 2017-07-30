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
package gobblin.audit.values.policy.column;

import java.lang.reflect.InvocationTargetException;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.audit.values.auditor.ValueAuditRuntimeMetadata;
import gobblin.util.ClassAliasResolver;

/**
 * Default factory class to create new {@link ColumnProjectionPolicy}s
 */
@Slf4j
public class DefaultColumnProjectionPolicyFactory {

  private static final String COLUMN_PROJECTION_POLICY_CLASS_NAME_KEY = "class";

  private final ClassAliasResolver<ColumnProjectionPolicy> aliasResolver;

  private DefaultColumnProjectionPolicyFactory() {
    this.aliasResolver = new ClassAliasResolver<>(ColumnProjectionPolicy.class);
  }

  /**
   * Create a new {@link ColumnProjectionPolicy} using the alias or cannonical classname specified at {@value #COLUMN_PROJECTION_POLICY_CLASS_NAME_KEY} in the <code>config</code>
   * The {@link ColumnProjectionPolicy} class MUST have an accessible constructor <code>abc(Config config, TableMetadata tableMetadata)</code>
   * <b>Note : Must have the key {@value #COLUMN_PROJECTION_POLICY_CLASS_NAME_KEY} set in <code>config</code> to create the {@link ColumnProjectionPolicy}</b>
   *
   * @param config job configs, Must have the key {@value #COLUMN_PROJECTION_POLICY_CLASS_NAME_KEY} set to create the {@link ColumnProjectionPolicy}
   * @param tableMetadata runtime table metadata
   *
   * @return a new instance of {@link ColumnProjectionPolicy}
   */
  public ColumnProjectionPolicy create(Config config, ValueAuditRuntimeMetadata.TableMetadata tableMetadata) {

    Preconditions.checkArgument(config.hasPath(COLUMN_PROJECTION_POLICY_CLASS_NAME_KEY));

    log.info("Using column projection class name/alias " + config.getString(COLUMN_PROJECTION_POLICY_CLASS_NAME_KEY));

    try {
      return (ColumnProjectionPolicy)ConstructorUtils.invokeConstructor(Class.forName(this.aliasResolver.resolve(
              config.getString(COLUMN_PROJECTION_POLICY_CLASS_NAME_KEY))), config, tableMetadata);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static class DefaultColumnProjectionPolicyFactoryHolder {
    private static final DefaultColumnProjectionPolicyFactory INSTANCE = new DefaultColumnProjectionPolicyFactory();
  }

  public static DefaultColumnProjectionPolicyFactory getInstance() {
    return DefaultColumnProjectionPolicyFactoryHolder.INSTANCE;
  }

}
