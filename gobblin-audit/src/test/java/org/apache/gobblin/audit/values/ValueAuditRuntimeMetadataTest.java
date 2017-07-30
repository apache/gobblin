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
import gobblin.audit.values.auditor.ValueAuditRuntimeMetadata;
import gobblin.audit.values.auditor.ValueAuditRuntimeMetadata.Phase;

import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = { "gobblin.audit.values" })
public class ValueAuditRuntimeMetadataTest {

  @Test
  public void testBuilderWithDefaults() throws Exception {
    Schema mockSchema = mock(Schema.class, Mockito.RETURNS_SMART_NULLS);
    ValueAuditRuntimeMetadata runtimeMetadata = ValueAuditRuntimeMetadata.builder("db", "t", mockSchema).build();

    Assert.assertEquals(runtimeMetadata.getTableMetadata().getDatabase(), "db");
    Assert.assertEquals(runtimeMetadata.getTableMetadata().getTable(), "t");
    Assert.assertEquals(runtimeMetadata.getTableMetadata().getTableSchema(), mockSchema);
    Assert.assertEquals(runtimeMetadata.getCluster(), "NA");
    Assert.assertEquals(runtimeMetadata.getDeltaId(), "NA");
    Assert.assertEquals(runtimeMetadata.getExtractId(), "NA");
    Assert.assertEquals(runtimeMetadata.getPartFileName(), "NA");
    Assert.assertEquals(runtimeMetadata.getSnapshotId(), "NA");
    Assert.assertEquals(runtimeMetadata.getPhase(), Phase.NA);

  }

  @Test
  public void testBuilder() throws Exception {
    Schema mockSchema = mock(Schema.class, Mockito.RETURNS_SMART_NULLS);
    ValueAuditRuntimeMetadata runtimeMetadata =
        ValueAuditRuntimeMetadata.builder("db", "t", mockSchema).cluster("c").deltaId("d").extractId("e")
            .partFileName("p").phase(Phase.AVRO_CONV).snapshotId("s").build();

    Assert.assertEquals(runtimeMetadata.getTableMetadata().getDatabase(), "db");
    Assert.assertEquals(runtimeMetadata.getTableMetadata().getTable(), "t");
    Assert.assertEquals(runtimeMetadata.getTableMetadata().getTableSchema(), mockSchema);
    Assert.assertEquals(runtimeMetadata.getCluster(), "c");
    Assert.assertEquals(runtimeMetadata.getDeltaId(), "d");
    Assert.assertEquals(runtimeMetadata.getExtractId(), "e");
    Assert.assertEquals(runtimeMetadata.getPartFileName(), "p");
    Assert.assertEquals(runtimeMetadata.getSnapshotId(), "s");
    Assert.assertEquals(runtimeMetadata.getPhase(), Phase.AVRO_CONV);

  }
}
