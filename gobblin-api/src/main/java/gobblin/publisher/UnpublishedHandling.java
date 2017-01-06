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

package gobblin.publisher;

import java.io.IOException;
import java.util.Collection;

import gobblin.configuration.WorkUnitState;


/**
 * {@link DataPublisher}s implementing this interface will cause Gobblin to call {@link #handleUnpublishedWorkUnits} if
 * it determines the job will not be published. This allows the publisher to run actions on partially successful jobs
 * (e.g. recovery, notification, etc.).
 */
public interface UnpublishedHandling {

  /**
   * This method will be called by Gobblin if it determines that a job should not be published.
   * @param states List of {@link WorkUnitState}s in the job.
   * @throws IOException
   */
  public void handleUnpublishedWorkUnits(Collection<? extends WorkUnitState> states) throws IOException;

}
