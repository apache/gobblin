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
package gobblin.compliance;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import gobblin.configuration.WorkUnitState;


@Test
public class HivePurgerConverterTest {
  private WorkUnitState stateMock = Mockito.mock(WorkUnitState.class);
  private ComplianceRecordSchema schemaMock = Mockito.mock(ComplianceRecordSchema.class);
  private HivePurgerConverter hivePurgerConverterMock = Mockito.mock(HivePurgerConverter.class);
  private ComplianceRecord recordMock = Mockito.mock(ComplianceRecord.class);

  @BeforeTest
  public void initialize() {
    Mockito.doCallRealMethod().when(this.hivePurgerConverterMock).convertSchema(this.schemaMock, this.stateMock);
  }

  public void convertSchemaTest() {
    Assert.assertNotNull(this.hivePurgerConverterMock.convertSchema(this.schemaMock, this.stateMock));
  }
}
