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
package gobblin.compliance;

import java.io.IOException;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import gobblin.configuration.WorkUnitState;
import gobblin.source.workunit.WorkUnit;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


public class HivePurgerExtractorTest {
  private ComplianceRecord recordMock = Mockito.mock(ComplianceRecord.class);
  private HivePurgerExtractor extractorMock = Mockito.mock(HivePurgerExtractor.class);

  @BeforeTest
  public void initialize()
      throws IOException {
    Mockito.doCallRealMethod().when(this.extractorMock).getSchema();
    Mockito.doCallRealMethod().when(this.extractorMock).readRecord(null);
    Mockito.doCallRealMethod().when(this.extractorMock).getExpectedRecordCount();
    Mockito.doCallRealMethod().when(this.extractorMock).setRecord(this.recordMock);
    this.extractorMock.setRecord(this.recordMock);
  }

  @Test
  public void getSchemaTest() {
    Assert.assertNotNull(this.extractorMock.getSchema());
  }

  @Test
  public void readRecordTest()
      throws IOException {
    Assert.assertNotNull(this.extractorMock.readRecord(null));
    Assert.assertNull(this.extractorMock.readRecord(null));
    Assert.assertEquals(1, this.extractorMock.getExpectedRecordCount());
  }
}
