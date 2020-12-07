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

package org.apache.gobblin.zuora;

import org.apache.gobblin.annotation.Alpha;


@Alpha
public class ZuoraConfigurationKeys {
  private ZuoraConfigurationKeys() {
  }

  public static final String ZUORA_OUTPUT_FORMAT = "zuora.output.format";
  public static final String ZUORA_API_NAME = "zuora.api.name";
  public static final String ZUORA_PARTNER = "zuora.partner";
  public static final String ZUORA_PROJECT = "zuora.project";

  /**
   * If you add a deleted column, for example, zuora.deleted.column=IsDeleted
   * the schema needs to be changed accordingly.
   * For example, the column below needs to be included as the first column in your schema definition
   * { "columnName":"IsDeleted", "isNullable":"FALSE", "dataType":{"type":"boolean"}, "comment":"" }
   *
   * Check the documentation at
   * https://knowledgecenter.zuora.com/DC_Developers/T_Aggregate_Query_API/BA_Stateless_and_Stateful_Modes
   */
  public static final String ZUORA_DELTED_COLUMN = "zuora.deleted.column";
  public static final String ZUORA_TIMESTAMP_COLUMNS = "zuora.timestamp.columns";
  public static final String ZUORA_ROW_LIMIT = "zuora.row.limit";

  public static final String ZUORA_API_RETRY_POST_COUNT = "zuora.api.retry.post.count";
  public static final String ZUORA_API_RETRY_POST_WAIT_TIME = "zuora.api.retry.post.wait_time";
  public static final String ZUORA_API_RETRY_GET_FILES_COUNT = "zuora.api.retry.get_files.count";
  public static final String ZUORA_API_RETRY_GET_FILES_WAIT_TIME = "zuora.api.retry.get_files.wait_time";
  public static final String ZUORA_API_RETRY_STREAM_FILES_COUNT = "zuora.api.retry.stream_files.count";
  public static final String ZUORA_API_RETRY_STREAM_FILES_WAIT_TIME = "zuora.api.retry.stream_files.wait_time";
}
