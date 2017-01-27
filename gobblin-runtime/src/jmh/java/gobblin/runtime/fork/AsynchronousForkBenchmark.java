/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.fork;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Control;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.runtime.ExecutionModel;
import gobblin.runtime.TaskContext;


@Warmup(iterations = 3)
@Measurement(iterations = 10)
@org.openjdk.jmh.annotations.Fork(value = 3)
@BenchmarkMode(value = Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AsynchronousForkBenchmark {
  @State(value = Scope.Group)
  public static class ForkState {
    private AsynchronousFork fork;

    @Setup
    public void setup() throws Exception {
      WorkUnitState state = new WorkUnitState();
      state.setProp(ConfigurationKeys.JOB_ID_KEY, "jobId");
      state.setProp(ConfigurationKeys.TASK_ID_KEY, "taskId");

      TaskContext taskContext = new MockTaskContext(state);
      fork = new AsynchronousFork(taskContext, String.class, 0, 0, ExecutionModel.BATCH);;
    }

    @TearDown
    public void tearDown() throws IOException {
      fork.close();
    }
  }

  @Benchmark
  @Group("processRecord")
  public void putRecord(Control control, ForkState forkState) throws Exception {
    if (!control.stopMeasurement) {
      forkState.fork.putRecord("a");
    }
  }

  @Benchmark
  @Group("processRecord")
  public void processRecord(Control control, ForkState forkState) throws Exception {
    if (!control.stopMeasurement) {
      forkState.fork.processRecord();
    }
  }
}