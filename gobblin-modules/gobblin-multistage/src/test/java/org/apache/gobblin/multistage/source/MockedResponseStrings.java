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

package org.apache.gobblin.multistage.source;

public class MockedResponseStrings {
  public MockedResponseStrings() {
  }

  public static final String mockedStringResponse =
      "[{\"id\":1111,\"site_id\":3333,\"uuid\":null,\"name\":\"dummyGHPExitSurvey\",\"active\":false,\"kind\":\"embed\",\"canonical_name\":null,\"created_at\":\"2017-09-1123:20:05+0000\",\"survey_url\":\"https://dummy/surveys/179513\",\"type\":\"survey\"}]";
  public static final String mockedStringResponseMultipleRecords = "[{\"id\":1111,\"site_id\":3333,\"uuid\":null,\"name\":\"dummyGHPExitSurvey\",\"active\":false,\"kind\":\"embed\",\"canonical_name\":null,\"created_at\":\"2017-09-1123:20:05+0000\",\"survey_url\":\"https://dummy/surveys/179513\",\"type\":\"survey\"},{\"id\":2222,\"site_id\":1234,\"uuid\":null,\"name\":\"dummyGHPExitSurvey\",\"active\":false,\"kind\":\"embed\",\"canonical_name\":null,\"created_at\":\"2017-09-1123:20:05+0000\",\"survey_url\":\"https://dummy/surveys/179513\",\"type\":\"survey\"}]";
}