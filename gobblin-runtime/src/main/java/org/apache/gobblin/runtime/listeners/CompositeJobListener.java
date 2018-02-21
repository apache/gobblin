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

package org.apache.gobblin.runtime.listeners;

import java.util.List;

import org.apache.gobblin.runtime.JobContext;

import com.google.common.collect.Lists;

import lombok.AllArgsConstructor;


@AllArgsConstructor
public class CompositeJobListener extends AbstractJobListener {
  private List<JobListener> listeners = Lists.newArrayList();

  public CompositeJobListener() {
  }

  public void addJobListener(JobListener listener) {
    this.listeners.add(listener);
  }


  @Override
  public void onJobPrepare(JobContext jobContext) throws Exception {
    String exceptions = "";
    for (JobListener listener: listeners) {
      try {
        listener.onJobPrepare(jobContext);
      } catch (Exception e) {
        exceptions += listener.getClass().getName() + ":" + e.toString();
      }
    }

    if (!exceptions.isEmpty()) {
      throw new RuntimeException(exceptions);
    }
  }

  @Override
  public void onJobStart(JobContext jobContext) throws Exception {
    String exceptions = "";
    for (JobListener listener: listeners) {
      try {
        listener.onJobStart(jobContext);
      } catch (Exception e) {
        exceptions += listener.getClass().getName() + ":" + e.toString();
      }
    }

    if (!exceptions.isEmpty()) {
      throw new RuntimeException(exceptions);
    }
  }

  @Override
  public void onJobCompletion(JobContext jobContext) throws Exception {
    String exceptions = "";
    for (JobListener listener: listeners) {
      try {
        listener.onJobCompletion(jobContext);
      } catch (Exception e) {
        exceptions += listener.getClass().getName() + ":" + e.toString();
      }
    }

    if (!exceptions.isEmpty()) {
      throw new RuntimeException(exceptions);
    }
  }

  @Override
  public void onJobCancellation(JobContext jobContext) throws Exception {
    String exceptions = "";
    for (JobListener listener: listeners) {
      try {
        listener.onJobCancellation(jobContext);
      } catch (Exception e) {
        exceptions += listener.getClass().getName() + ":" + e.toString();
      }
    }

    if (!exceptions.isEmpty()) {
      throw new RuntimeException(exceptions);
    }
  }

  @Override
  public void onJobFailure(JobContext jobContext) throws Exception {
    String exceptions = "";
    for (JobListener listener: listeners) {
      try {
        listener.onJobFailure(jobContext);
      } catch (Exception e) {
        exceptions += listener.getClass().getName() + ":" + e.toString();
      }
    }

    if (!exceptions.isEmpty()) {
      throw new RuntimeException(exceptions);
    }
  }


}
