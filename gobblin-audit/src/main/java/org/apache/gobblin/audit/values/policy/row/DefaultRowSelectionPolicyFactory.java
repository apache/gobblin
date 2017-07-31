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
package org.apache.gobblin.audit.values.policy.row;

import java.lang.reflect.InvocationTargetException;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import org.apache.gobblin.audit.values.auditor.ValueAuditGenerator;
import org.apache.gobblin.audit.values.auditor.ValueAuditRuntimeMetadata;
import org.apache.gobblin.audit.values.policy.column.ColumnProjectionPolicy;
import org.apache.gobblin.util.ClassAliasResolver;

/**
 * Default factory class to create new {@link RowSelectionPolicy}s
 */
@Slf4j
public class DefaultRowSelectionPolicyFactory {

  private static final String ROW_SELECTION_POLICY_CLASS_NAME_KEY = "class";

  private final ClassAliasResolver<RowSelectionPolicy> aliasResolver;

  private DefaultRowSelectionPolicyFactory() {
    this.aliasResolver = new ClassAliasResolver<>(RowSelectionPolicy.class);
  }

  /**
   * Create a new {@link RowSelectionPolicy} using the alias or cannonical classname specified at {@value #ROW_SELECTION_POLICY_CLASS_NAME_KEY} in the <code>config</code>
   * The {@link RowSelectionPolicy} class MUST have an accessible constructor <code>abc(Config config, TableMetadata tableMetadata, ColumnProjectionPolicy columnProjectionPolicy)</code>
   * <b>Note : must have the key {@value #ROW_SELECTION_POLICY_CLASS_NAME_KEY} set in <code>config</code> to create the {@link RowSelectionPolicy}</b>
   *
   * @param config job configs, must have the key {@value #ROW_SELECTION_POLICY_CLASS_NAME_KEY} set to create the {@link RowSelectionPolicy}
   * @param tableMetadata runtime table metadata
   * @param columnProjectionPolicy used by the {@link ValueAuditGenerator}
   *
   * @return a new instance of {@link RowSelectionPolicy}
   */
  public RowSelectionPolicy create(Config config, ValueAuditRuntimeMetadata.TableMetadata tableMetadata, ColumnProjectionPolicy columnProjectionPolicy) {

    Preconditions.checkArgument(config.hasPath(ROW_SELECTION_POLICY_CLASS_NAME_KEY));

    log.info("Using row selection class name/alias " + config.getString(ROW_SELECTION_POLICY_CLASS_NAME_KEY));

    try {
      return (RowSelectionPolicy)ConstructorUtils.invokeConstructor(Class.forName(this.aliasResolver.resolve(
              config.getString(ROW_SELECTION_POLICY_CLASS_NAME_KEY))), config, tableMetadata, columnProjectionPolicy);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static class DefaultRowSelectionPolicyFactoryHolder {
    private static final DefaultRowSelectionPolicyFactory INSTANCE = new DefaultRowSelectionPolicyFactory();
  }

  public static DefaultRowSelectionPolicyFactory getInstance() {
    return DefaultRowSelectionPolicyFactoryHolder.INSTANCE;
  }

}

