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

package org.apache.gobblin.compaction.mapreduce.test;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

import org.apache.gobblin.compaction.mapreduce.CompactionJobConfigurator;
import org.apache.gobblin.compaction.mapreduce.CompactionOrcJobConfigurator;
import org.apache.gobblin.configuration.State;


public class TestCompactionOrcJobConfigurator extends CompactionOrcJobConfigurator {
  public static class Factory implements CompactionJobConfigurator.ConfiguratorFactory {
    @Override
    public TestCompactionOrcJobConfigurator createConfigurator(State state) throws IOException {
      return new TestCompactionOrcJobConfigurator(state);
    }
  }

  @Override
  protected void setNumberOfReducers(Job job) {
    job.setNumReduceTasks(1);
  }

  public TestCompactionOrcJobConfigurator(State state) throws IOException {
    super(state);
  }
}