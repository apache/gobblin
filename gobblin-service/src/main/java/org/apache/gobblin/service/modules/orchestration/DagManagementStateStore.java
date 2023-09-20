package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An interface to provide abstractions for managing {@link Dag} and {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} states
 */
public interface DagManagementStateStore {

  public void addJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode);

  public void deleteJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode);

  public boolean hasRunningJobs(String dagId);

  public void removeDagActionFromStore(DagManager.DagId dagId, DagActionStore.FlowActionType flowActionType) throws IOException;

  public Map<String, LinkedList<Dag.DagNode<JobExecutionPlan>>> getDagToJobs();

  public Map<String, Dag<JobExecutionPlan>> getDagIdToDags();

  public Map<String, Long> getDagToSLA();

  //TODO: Add get methods for dags and jobs

  }
