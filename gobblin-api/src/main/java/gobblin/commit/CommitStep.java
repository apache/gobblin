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

package gobblin.commit;

import java.io.IOException;

import gobblin.annotation.Alpha;


/**
 * A step during committing a dataset that should be executed atomically with other steps under exactly-once semantics.
 * An example is publishing the data files of a dataset, which should be executed atomically with persisting the
 * dataset state in order to avoid pulling duplicate data.
 *
 * @author Ziyang Liu
 */
@Alpha
public interface CommitStep {

  /**
   * Determine whether the commit step has been completed.
   */
  public boolean isCompleted() throws IOException;

  /**
   * Execute the commit step.
   */
  public void execute() throws IOException;
}
