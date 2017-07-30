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
package gobblin.audit.values;

import static org.mockito.Mockito.mock;
import gobblin.audit.values.auditor.ValueAuditGenerator;
import gobblin.audit.values.auditor.ValueAuditRuntimeMetadata;
import gobblin.audit.values.policy.column.ProjectAllColumnProjectionPolicy;
import gobblin.audit.values.policy.row.SelectAllRowSelectionPolicy;

import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@Test(groups = {"gobblin.audit.values"})
public class ValueAuditGeneratorTest {

  @Test
  public void testConstructor() throws Exception {

    Config config =ConfigFactory.parseMap(ImmutableMap.of(
            "columnProjection.class", ProjectAllColumnProjectionPolicy.class.getCanonicalName(),
            "rowSelection.class", SelectAllRowSelectionPolicy.class.getCanonicalName(),
            "auditSink.class", MockSink.class.getCanonicalName()));
    ValueAuditRuntimeMetadata runtimeMetadata = mock(ValueAuditRuntimeMetadata.class, Mockito.RETURNS_SMART_NULLS);
    ValueAuditGenerator auditGenerator = ValueAuditGenerator.create(config, runtimeMetadata);
    auditGenerator.audit(mock(GenericRecord.class, Mockito.RETURNS_SMART_NULLS));
    Assert.assertEquals(auditGenerator.getRowSelectionPolicy().getClass().getCanonicalName(), SelectAllRowSelectionPolicy.class.getCanonicalName());
    Assert.assertEquals(auditGenerator.getColumnProjectionPolicy().getClass().getCanonicalName(), ProjectAllColumnProjectionPolicy.class.getCanonicalName());
    Assert.assertEquals(auditGenerator.getAuditSink().getClass().getCanonicalName(), MockSink.class.getCanonicalName());
  }

  @Test
  public void testConstructorWithAlias() throws Exception {

    Config config =ConfigFactory.parseMap(ImmutableMap.of(
            "columnProjection.class", "ProjectAll",
            "rowSelection.class", "SelectAll",
            "auditSink.class", "MockSink"));
    ValueAuditRuntimeMetadata runtimeMetadata = mock(ValueAuditRuntimeMetadata.class, Mockito.RETURNS_SMART_NULLS);
    ValueAuditGenerator auditGenerator = ValueAuditGenerator.create(config, runtimeMetadata);
    auditGenerator.audit(mock(GenericRecord.class, Mockito.RETURNS_SMART_NULLS));
    Assert.assertEquals(auditGenerator.getRowSelectionPolicy().getClass().getCanonicalName(), SelectAllRowSelectionPolicy.class.getCanonicalName());
    Assert.assertEquals(auditGenerator.getColumnProjectionPolicy().getClass().getCanonicalName(), ProjectAllColumnProjectionPolicy.class.getCanonicalName());
    Assert.assertEquals(auditGenerator.getAuditSink().getClass().getCanonicalName(), MockSink.class.getCanonicalName());
  }
}
