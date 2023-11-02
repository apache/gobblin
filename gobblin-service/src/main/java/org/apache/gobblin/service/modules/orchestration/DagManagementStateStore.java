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

  public void addDagSLA(String dagId, Long flowSla);

  public Long getDagSLA(String dagId);

  public Dag<JobExecutionPlan> getDag(String dagId);

  public LinkedList<Dag.DagNode<JobExecutionPlan>> getJobs(String dagId) throws IOException;

  public boolean addFailedDagId(String dagId);

  public boolean checkFailedDagId(String dagId);

  public boolean addCleanUpDagId(String dagId);

  public boolean checkCleanUpDagId(String dagId);


  //TODO: Add get methods for dags and jobs

  }
