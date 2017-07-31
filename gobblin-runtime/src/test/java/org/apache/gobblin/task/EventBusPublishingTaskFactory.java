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

package org.apache.gobblin.task;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.testng.Assert;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.publisher.DataPublisher;
import org.apache.gobblin.publisher.NoopPublisher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.task.BaseAbstractTask;
import org.apache.gobblin.runtime.task.TaskFactory;
import org.apache.gobblin.runtime.task.TaskIFace;
import org.apache.gobblin.runtime.task.TaskUtils;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.writer.test.TestingEventBuses;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;


public class EventBusPublishingTaskFactory implements TaskFactory {
  public static final String TASK_ID_KEY = "MyFactory.task.id";
  public static final String EVENTBUS_ID_KEY = "eventbus.id";
  public static final String RUN_EVENT = "run";
  public static final String COMMIT_EVENT = "commit";
  public static final String PUBLISH_EVENT = "publish";
  public static final String PREVIOUS_STATE_EVENT = "previousState";

  @Override
  public TaskIFace createTask(TaskContext taskContext) {
    String taskId = taskContext.getTaskState().getProp(TASK_ID_KEY);
    EventBus eventBus = null;
    if (taskContext.getTaskState().contains(EVENTBUS_ID_KEY)) {
      String eventbusId = taskContext.getTaskState().getProp(EVENTBUS_ID_KEY);
      eventBus = TestingEventBuses.getEventBus(eventbusId);
    }
    return new Task(taskContext, taskId, eventBus);
  }

  @Override
  public DataPublisher createDataPublisher(JobState.DatasetState datasetState) {
    EventBus eventBus = null;
    if (datasetState.getTaskStates().get(0).contains(EVENTBUS_ID_KEY)) {
      eventBus = TestingEventBuses.getEventBus(datasetState.getTaskStates().get(0).getProp(EVENTBUS_ID_KEY));
    }
    return new Publisher(datasetState, eventBus);
  }

  public static class EventListener {
    @Getter
    private final Queue<Event> events = Queues.newArrayDeque();

    @Subscribe
    public void listen(Event event) {
      this.events.add(event);
    }

    public SetMultimap<String, Integer> getEventsSeenMap() {
      SetMultimap<String, Integer> seenEvents = HashMultimap.create();
      for (EventBusPublishingTaskFactory.Event event : this.events) {
        seenEvents.put(event.getType(), event.getId());
      }
      return seenEvents;
    }
  }

  public static class Source implements org.apache.gobblin.source.Source<String, String> {
    public static final String NUM_TASKS_KEY = "num.tasks";

    @Override
    public List<WorkUnit> getWorkunits(SourceState state) {
      int numTasks = state.getPropAsInt(NUM_TASKS_KEY);
      String eventBusId = state.getProp(EVENTBUS_ID_KEY);
      EventBus eventBus = TestingEventBuses.getEventBus(eventBusId);

      Map<String, SourceState> previousStates = state.getPreviousDatasetStatesByUrns();
      for (Map.Entry<String, SourceState> entry : previousStates.entrySet()) {
        JobState.DatasetState datasetState = (JobState.DatasetState) entry.getValue();
        for (TaskState taskState : datasetState.getTaskStates()) {
          if (taskState.contains(Task.PERSISTENT_STATE) && eventBus != null) {
            eventBus.post(new Event(PREVIOUS_STATE_EVENT, taskState.getPropAsInt(Task.PERSISTENT_STATE)));
          }
        }
      }

      List<WorkUnit> workUnits = Lists.newArrayList();
      for (int i = 0; i < numTasks; i++) {
        workUnits.add(createWorkUnit(i, eventBusId));
      }
      return workUnits;
    }

    protected WorkUnit createWorkUnit(int wuNumber, String eventBusId) {
      WorkUnit workUnit = new WorkUnit();
      TaskUtils.setTaskFactoryClass(workUnit, EventBusPublishingTaskFactory.class);
      workUnit.setProp(EVENTBUS_ID_KEY, eventBusId);
      workUnit.setProp(TASK_ID_KEY, wuNumber);
      return workUnit;
    }

    @Override
    public Extractor<String, String> getExtractor(WorkUnitState state) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown(SourceState state) {
      // Do nothing
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static class Task extends BaseAbstractTask {
    public static final String PERSISTENT_STATE = "persistent.state";
    public static final String EXECUTION_METADATA = "execution.metadata";

    private final String taskId;
    private final EventBus eventBus;

    public Task(TaskContext taskContext, String taskId, EventBus eventBus) {
      super(taskContext);
      this.taskId = taskId;
      this.eventBus = eventBus;
    }

    @Override
    public void run() {
      if (this.eventBus != null) {
        this.eventBus.post(new Event(RUN_EVENT, Integer.parseInt(this.taskId)));
      }
      super.run();
    }

    @Override
    public void commit() {
      if (this.eventBus != null) {
        this.eventBus.post(new Event(COMMIT_EVENT, Integer.parseInt(this.taskId)));
      }
      super.commit();
    }

    @Override
    public State getPersistentState() {
      State state = super.getPersistentState();
      state.setProp(PERSISTENT_STATE, this.taskId);
      return state;
    }

    @Override
    public State getExecutionMetadata() {
      State state = super.getExecutionMetadata();
      state.setProp(EXECUTION_METADATA, this.taskId);
      return state;
    }
  }

  public static class Publisher extends NoopPublisher {
    private final EventBus eventBus;

    public Publisher(State state, EventBus eventBus) {
      super(state);
      this.eventBus = eventBus;
    }

    @Override
    public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
      for (WorkUnitState state : states) {
        String taskId = state.getProp(TASK_ID_KEY);
        Assert.assertEquals(state.getProp(Task.EXECUTION_METADATA), taskId);
        Assert.assertEquals(state.getProp(Task.PERSISTENT_STATE), taskId);
        if (this.eventBus != null) {
          this.eventBus.post(new Event(PUBLISH_EVENT, Integer.parseInt(state.getProp(TASK_ID_KEY))));
        }
      }
      super.publishData(states);
    }
  }

  @Data
  public static class Event {
    private final String type;
    private final int id;
  }
}
