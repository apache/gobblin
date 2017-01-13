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
package gobblin.runtime.std;

import com.google.common.base.Optional;

import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecSchedule;

import lombok.Data;

/**
 * Simple POJO implementation of {@link JobSpecSchedule}
 */
@Data
public class DefaultJobSpecScheduleImpl implements JobSpecSchedule {
  private final JobSpec jobSpec;
  private final Runnable jobRunnable;
  private final Optional<Long> nextRunTimeMillis;

  /** Creates a schedule denoting that the job is to be executed immediately */
  public static DefaultJobSpecScheduleImpl createImmediateSchedule(JobSpec jobSpec,
                                                                   Runnable jobRunnable) {
    return new DefaultJobSpecScheduleImpl(jobSpec, jobRunnable,
                                          Optional.of(System.currentTimeMillis()));
  }

  /** Creates a schedule denoting that the job is not to be executed */
  public static DefaultJobSpecScheduleImpl createNoSchedule(JobSpec jobSpec,
                                                            Runnable jobRunnable) {
    return new DefaultJobSpecScheduleImpl(jobSpec, jobRunnable,
                                          Optional.<Long>absent());
  }

}
