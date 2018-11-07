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

import java.io.IOException;
import java.util.List;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.extract.Command;
import org.apache.gobblin.source.extractor.extract.CommandOutput;
import org.apache.gobblin.source.extractor.extract.restapi.RestApiCommand;
import org.apache.gobblin.source.extractor.watermark.Predicate;


@Alpha
public interface ZuoraClient {

  List<Command> buildPostCommand(List<Predicate> predicateList);

  CommandOutput<RestApiCommand, String> executePostRequest(final Command command)
      throws DataRecordException;

  List<String> getFileIds(final String jobId)
      throws DataRecordException, IOException;

  CommandOutput<RestApiCommand, String> executeGetRequest(final Command cmd)
      throws Exception;

  String getEndPoint(String relativeUrl);
}
