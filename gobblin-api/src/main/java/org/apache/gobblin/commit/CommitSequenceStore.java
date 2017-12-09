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

package org.apache.gobblin.commit;

import java.io.IOException;
import java.util.Collection;

import com.google.common.base.Optional;

import org.apache.gobblin.annotation.Alpha;


/**
 * A store for {@link CommitSequence}s. A {@link CommitSequence} is identified by job name and dataset URN.
 *
 * @author Ziyang Liu
 */
@Alpha
public interface CommitSequenceStore {

  /**
   * Whether a {@link CommitSequence} with the given job name exists in the store.
   */
  boolean exists(String jobName) throws IOException;

  /**
   * Whether a {@link CommitSequence} with the given job name and dataset URN exists in a store.
   */
  public boolean exists(String jobName, String datasetUrn) throws IOException;

  /**
   * delete a given job name from the store along with all {@link CommitSequence}s associated with this job.
   */
  public void delete(String jobName) throws IOException;

  /**
   * delete the {@link CommitSequence} for the given job name and dataset URN.
   */
  public void delete(String jobName, String datasetUrn) throws IOException;

  /**
   * Put a {@link CommitSequence} with the given job name and dataset URN.
   *
   * @throws IOException if a {@link CommitSequence} for the given job name and dataset URN exists in the store.
   */
  public void put(String jobName, String datasetUrn, CommitSequence commitSequence) throws IOException;

  /**
   * Get a {@link Collection} of dataset URNs with the given job name.
   */
  public Collection<String> get(String jobName) throws IOException;

  /**
   * Get the {@link CommitSequence} associated with the given job name and dataset URN.
   */
  public Optional<CommitSequence> get(String jobName, String datasetUrn) throws IOException;

}
