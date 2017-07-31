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
package org.apache.gobblin.audit.values.sink;

import java.lang.reflect.InvocationTargetException;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.typesafe.config.Config;

import org.apache.gobblin.audit.values.auditor.ValueAuditRuntimeMetadata;
import org.apache.gobblin.util.ClassAliasResolver;

/**
 * Default factory class to create new {@link AuditSink}s
 */
@Slf4j
public class DefaultAuditSinkFactory {

  private static final String AUDIT_SINK_CLASS_NAME_KEY = "class";
  private static final String DEFAULT_AUDIT_SINK_CLASS =  FsAuditSink.class.getCanonicalName();

  private final ClassAliasResolver<AuditSink> aliasResolver;

  private DefaultAuditSinkFactory() {
    this.aliasResolver = new ClassAliasResolver<>(AuditSink.class);
  }

  /**
   * Create a new {@link AuditSink} using the alias or cannonical classname specified at {@value #AUDIT_SINK_CLASS_NAME_KEY} in the <code>config</code>
   * The {@link AuditSink} class MUST have an accessible constructor <code>abc(Config config, TableMetadata tableMetadata)</code>
   * <br>
   * If {@value #AUDIT_SINK_CLASS_NAME_KEY} is not set in <code>config</code>, a default {@link #DEFAULT_AUDIT_SINK_CLASS} is used
   *
   * @param config job configs
   * @param auditRuntimeMetadata runtime table metadata
   *
   * @return a new instance of {@link AuditSink}
   */
  public AuditSink create(Config config, ValueAuditRuntimeMetadata auditRuntimeMetadata) {

    String sinkClassName = DEFAULT_AUDIT_SINK_CLASS;
    if (config.hasPath(AUDIT_SINK_CLASS_NAME_KEY)) {
      sinkClassName = config.getString(AUDIT_SINK_CLASS_NAME_KEY);
    }
    log.info("Using audit sink class name/alias " + sinkClassName);

    try {
      return (AuditSink)ConstructorUtils.invokeConstructor(Class.forName(this.aliasResolver.resolve(
          sinkClassName)), config, auditRuntimeMetadata);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static class DefaultAuditSinkFactoryHolder {
    private static final DefaultAuditSinkFactory INSTANCE = new DefaultAuditSinkFactory();
  }

  public static DefaultAuditSinkFactory getInstance() {
    return DefaultAuditSinkFactoryHolder.INSTANCE;
  }

}
