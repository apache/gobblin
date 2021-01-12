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

package org.apache.gobblin.multistage.connection;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import gobblin.configuration.SourceState;
import java.io.UnsupportedEncodingException;
import org.apache.gobblin.multistage.connection.MultistageReadConnection;
import org.apache.gobblin.multistage.exception.RetriableAuthenticationException;
import org.apache.gobblin.multistage.keys.ExtractorKeys;
import org.apache.gobblin.multistage.keys.JobKeys;
import org.apache.gobblin.multistage.util.VariableUtils;
import org.apache.gobblin.multistage.util.WorkUnitStatus;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.powermock.api.mockito.PowerMockito.*;


@Test
@PrepareForTest(VariableUtils.class)
public class MulstistageReadConnectionTest extends PowerMockTestCase {
  @Test
  public void testGetNext() throws RetriableAuthenticationException {
    MultistageReadConnection conn = new MultistageReadConnection(new SourceState(), new JobKeys(), new ExtractorKeys());
    conn.getExtractorKeys().setSignature("testSignature");
    conn.getExtractorKeys().setActivationParameters(new JsonObject());

    WorkUnitStatus workUnitStatus = Mockito.mock(WorkUnitStatus.class);
    WorkUnitStatus.WorkUnitStatusBuilder builder = Mockito.mock(WorkUnitStatus.WorkUnitStatusBuilder.class);
    when(builder.build()).thenReturn(workUnitStatus);
    when(workUnitStatus.toBuilder()).thenReturn(builder);
    Assert.assertEquals(conn.getNext(workUnitStatus), workUnitStatus);

    // cover the exception branch
    JobKeys jobKeys = Mockito.mock(JobKeys.class);
    when(jobKeys.getCallInterval()).thenReturn(1L);
    conn.setJobKeys(jobKeys);
    when(jobKeys.getSourceParameters()).thenReturn(new JsonArray());
    when(jobKeys.getCallInterval()).thenThrow(Mockito.mock(IllegalArgumentException.class));
    conn.getNext(workUnitStatus);
    Assert.assertEquals(conn.getNext(workUnitStatus), workUnitStatus);
  }

  @Test
  public void testGetWorkUnitSpecificString() throws UnsupportedEncodingException {
    // Test normal case
    MultistageReadConnection conn = new MultistageReadConnection(new SourceState(), new JobKeys(), new ExtractorKeys());
    String template = "test_template";
    JsonObject obj = new JsonObject();
    Assert.assertEquals(conn.getWorkUnitSpecificString(template, obj), template);

    // Test exception by PowerMock
    PowerMockito.mockStatic(VariableUtils.class);
    when(VariableUtils.replaceWithTracking(template, obj, false)).thenThrow(UnsupportedEncodingException.class);
    Assert.assertEquals(conn.getWorkUnitSpecificString(template, obj), template);
  }
}
