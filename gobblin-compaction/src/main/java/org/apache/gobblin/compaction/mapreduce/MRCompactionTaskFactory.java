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
package org.apache.gobblin.compaction.mapreduce;

import java.io.IOException;


import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.mapreduce.MRTaskFactory;
import org.apache.gobblin.runtime.task.TaskIFace;

/**
 * A subclass of {@link MRTaskFactory} which provides a customized {@link MRCompactionTask} instance
 */
public class MRCompactionTaskFactory extends MRTaskFactory {
  @Override
  public TaskIFace createTask(TaskContext taskContext) {
    try {
      return new MRCompactionTask(taskContext);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
