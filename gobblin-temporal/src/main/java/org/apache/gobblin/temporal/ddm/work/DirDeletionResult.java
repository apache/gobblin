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

package org.apache.gobblin.temporal.ddm.work;

import java.util.HashMap;
import java.util.Map;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/**
 * Data structure representing the stats for a cleaned up work directory, where it returns a map of directories the result of their cleanup
 */
@Data
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@RequiredArgsConstructor
public class DirDeletionResult {

  @NonNull private Map<String, Boolean> successesByDirPath;

  /**
   * Empty result that should be used instead of empty constructor and needed to support jackson (de)serialization, otherwise will face the following error
   * Caused by: io.temporal.common.converter.DataConverterException: com.fasterxml.jackson.databind.JsonMappingException: successesByDirPath is marked non-null but is null
   *  at [Source: (byte[])"{"successesByDirPath":null}"; line: 1, column: 23] (through reference chain: org.apache.gobblin.temporal.ddm.work.DirDeletionResult["successesByDirPath"])
   * 	at io.temporal.common.converter.JacksonJsonPayloadConverter.fromData(JacksonJsonPayloadConverter.java:101)
   * 	at io.temporal.common.converter.DefaultDataConverter.fromPayload(DefaultDataConverter.java:145)
   * @return
   */
  public static DirDeletionResult createEmpty() {
    return new DirDeletionResult(new HashMap<>());
  }
}
