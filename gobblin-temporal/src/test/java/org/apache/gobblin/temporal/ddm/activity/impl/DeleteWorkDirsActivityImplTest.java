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

package org.apache.gobblin.temporal.ddm.activity.impl;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.temporal.api.common.v1.Payload;
import io.temporal.common.converter.JacksonJsonPayloadConverter;
import io.temporal.common.converter.PayloadConverter;

import org.apache.gobblin.temporal.ddm.activity.DeleteWorkDirsActivity;
import org.apache.gobblin.temporal.ddm.work.DirDeletionResult;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;


public class DeleteWorkDirsActivityImplTest {

  @Test
  public void testEmptyDeleteSupportsSerde() {
    DeleteWorkDirsActivity deleteWorkDirsActivity = new DeleteWorkDirsActivityImpl();
    WUProcessingSpec workSpec = new WUProcessingSpec();
    Set<String> workDirPaths = new HashSet<>();
    DirDeletionResult result = deleteWorkDirsActivity.delete(workSpec, null, workDirPaths);
    PayloadConverter converter = new JacksonJsonPayloadConverter();
    Optional<Payload> payload = converter.toData(result);
    DirDeletionResult result2 = converter.fromData(payload.get(), DirDeletionResult.class, DirDeletionResult.class);
    Assert.assertEquals(result, result2);
  }
}
