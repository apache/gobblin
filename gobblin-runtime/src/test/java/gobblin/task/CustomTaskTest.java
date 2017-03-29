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

package gobblin.task;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Files;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.publisher.DataPublisher;
import gobblin.publisher.NoopPublisher;
import gobblin.runtime.JobState;
import gobblin.runtime.TaskContext;
import gobblin.runtime.TaskState;
import gobblin.runtime.api.JobExecutionResult;
import gobblin.runtime.embedded.EmbeddedGobblin;
import gobblin.runtime.task.BaseAbstractTask;
import gobblin.runtime.task.TaskFactory;
import gobblin.runtime.task.TaskIFace;
import gobblin.runtime.task.TaskUtils;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;
import gobblin.writer.test.TestingEventBuses;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CustomTaskTest {

  @Test
  public void testCustomTask() throws Exception {

    String eventBusId = UUID.randomUUID().toString();
    MyListener listener = new MyListener();

    EventBus eventBus = TestingEventBuses.getEventBus(eventBusId);
    eventBus.register(listener);

    JobExecutionResult result =
        new EmbeddedGobblin("testJob").setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY, MySource.class.getName())
        .setConfiguration(MySource.NUM_TASKS_KEY, "10").setConfiguration(MyFactory.EVENTBUS_ID_KEY, eventBusId)
        .run();

    Assert.assertTrue(result.isSuccessful());

    SetMultimap<String, Integer> seenEvents = HashMultimap.create();
    Set<Integer> expected = Sets.newHashSet(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);


    for (Event event : listener.events) {
      seenEvents.put(event.getType(), event.getId());
    }

    Assert.assertEquals(seenEvents.get("run"), expected);
    Assert.assertEquals(seenEvents.get("commit"), expected);
    Assert.assertEquals(seenEvents.get("publish"), expected);
  }

  @Test
  public void testStatePersistence() throws Exception {
    File stateStore = Files.createTempDir();
    stateStore.deleteOnExit();

    String eventBusId = UUID.randomUUID().toString();
    MyListener listener = new MyListener();

    EventBus eventBus = TestingEventBuses.getEventBus(eventBusId);
    eventBus.register(listener);

    EmbeddedGobblin embeddedGobblin = new EmbeddedGobblin("testJob")
        .setConfiguration(MyFactory.EVENTBUS_ID_KEY, eventBusId)
        .setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY, MySource.class.getName())
        .setConfiguration(MySource.NUM_TASKS_KEY, "2")
        .setConfiguration(ConfigurationKeys.STATE_STORE_ENABLED, Boolean.toString(true))
        .setConfiguration(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, stateStore.getAbsolutePath());

    JobExecutionResult result = embeddedGobblin.run();
    Assert.assertTrue(result.isSuccessful());

    SetMultimap<String, Integer> seenEvents = HashMultimap.create();
    for (Event event : listener.events) {
      seenEvents.put(event.getType(), event.getId());
    }
    Assert.assertEquals(seenEvents.get("previousState").size(), 0);

    result = embeddedGobblin.run();
    Assert.assertTrue(result.isSuccessful());

    seenEvents = HashMultimap.create();
    for (Event event : listener.events) {
      seenEvents.put(event.getType(), event.getId());
    }

    Assert.assertEquals(seenEvents.get("previousState"), Sets.newHashSet(0, 1));

  }

  public static class MyListener {
    private final Queue<Event> events = Queues.newArrayDeque();

    @Subscribe
    public void listen(Event event) {
      this.events.add(event);
    }
  }

  public static class MySource implements Source<String, String> {
    public static final String NUM_TASKS_KEY = "num.tasks";

    @Override
    public List<WorkUnit> getWorkunits(SourceState state) {
      int numTasks = state.getPropAsInt(NUM_TASKS_KEY);
      String eventBusId = state.getProp(MyFactory.EVENTBUS_ID_KEY);
      EventBus eventBus = TestingEventBuses.getEventBus(eventBusId);

      Map<String, SourceState> previousStates = state.getPreviousDatasetStatesByUrns();
      for (Map.Entry<String, SourceState> entry : previousStates.entrySet()) {
        JobState.DatasetState datasetState = (JobState.DatasetState) entry.getValue();
        for (TaskState taskState : datasetState.getTaskStates()) {
          if (taskState.contains(MyTask.PERSISTENT_STATE) && eventBus != null) {
            eventBus.post(new Event("previousState", taskState.getPropAsInt(MyTask.PERSISTENT_STATE)));
          }
        }
      }

      List<WorkUnit> workUnits = Lists.newArrayList();
      for (int i = 0; i < numTasks; i++) {
        WorkUnit workUnit = new WorkUnit();
        TaskUtils.setTaskFactoryClass(workUnit, MyFactory.class);
        workUnit.setProp(MyFactory.EVENTBUS_ID_KEY, eventBusId);
        workUnit.setProp(MyFactory.TASK_ID_KEY, i);
        workUnits.add(workUnit);
      }
      return workUnits;
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
  public static class MyTask extends BaseAbstractTask {
    public static final String PERSISTENT_STATE = "persistent.state";
    public static final String EXECUTION_METADATA = "execution.metadata";

    private final String taskId;
    private final EventBus eventBus;

    public MyTask(TaskContext taskContext, String taskId, EventBus eventBus) {
      super(taskContext);
      this.taskId = taskId;
      this.eventBus = eventBus;
    }

    @Override
    public void run() {
      if (this.eventBus != null) {
        this.eventBus.post(new Event("run", Integer.parseInt(this.taskId)));
      }
      super.run();
    }

    @Override
    public void commit() {
      if (this.eventBus != null) {
        this.eventBus.post(new Event("commit", Integer.parseInt(this.taskId)));
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

  public static class MyPublisher extends NoopPublisher {
    private final EventBus eventBus;

    public MyPublisher(State state, EventBus eventBus) {
      super(state);
      this.eventBus = eventBus;
    }

    @Override
    public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
      for (WorkUnitState state : states) {
        String taskId = state.getProp(MyFactory.TASK_ID_KEY);
        Assert.assertEquals(state.getProp(MyTask.EXECUTION_METADATA), taskId);
        Assert.assertEquals(state.getProp(MyTask.PERSISTENT_STATE), taskId);
        if (this.eventBus != null) {
          this.eventBus.post(new Event("publish", Integer.parseInt(state.getProp(MyFactory.TASK_ID_KEY))));
        }
      }
      super.publishData(states);
    }
  }

  public static class MyFactory implements TaskFactory {
    public static final String TASK_ID_KEY = "MyFactory.task.id";
    public static final String EVENTBUS_ID_KEY = "eventbus.id";

    @Override
    public TaskIFace createTask(TaskContext taskContext) {
      String taskId = taskContext.getTaskState().getProp(TASK_ID_KEY);
      EventBus eventBus = null;
      if (taskContext.getTaskState().contains(EVENTBUS_ID_KEY)) {
        String eventbusId = taskContext.getTaskState().getProp(EVENTBUS_ID_KEY);
        eventBus = TestingEventBuses.getEventBus(eventbusId);
      }
      return new MyTask(taskContext, taskId, eventBus);
    }

    @Override
    public DataPublisher createDataPublisher(JobState.DatasetState datasetState) {
      EventBus eventBus = null;
      if (datasetState.getTaskStates().get(0).contains(EVENTBUS_ID_KEY)) {
        eventBus = TestingEventBuses.getEventBus(datasetState.getTaskStates().get(0).getProp(EVENTBUS_ID_KEY));
      }
      return new MyPublisher(datasetState, eventBus);
    }
  }

  @Data
  public static class Event {
    private final String type;
    private final int id;
  }

}
