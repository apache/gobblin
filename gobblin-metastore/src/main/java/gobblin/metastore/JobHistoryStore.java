/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metastore;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import gobblin.rest.JobExecutionInfo;
import gobblin.rest.JobExecutionQuery;


/**
 * An interface for stores that store job execution information.
 *
 * @author ynli
 */
public interface JobHistoryStore extends Closeable {

  /**
   * Insert a new or update an existing job execution record.
   *
   * @param jobExecutionInfo a {@link JobExecutionInfo} record
   * @throws java.io.IOException if the insertion or update fails
   */
  public void put(JobExecutionInfo jobExecutionInfo)
      throws IOException;

  /**
   * Get a list of {@link JobExecutionInfo} records as results of the given query.
   *
   * @param query a {@link JobExecutionQuery} instance
   * @return a list of {@link JobExecutionInfo} records
   * @throws IOException if the query fails
   */
  public List<JobExecutionInfo> get(JobExecutionQuery query)
      throws IOException;
}
