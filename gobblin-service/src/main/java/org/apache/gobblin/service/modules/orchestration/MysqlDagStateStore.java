package org.apache.gobblin.service.modules.orchestration;

import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;
import com.typesafe.config.Config;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.MysqlStateStore;
import org.apache.gobblin.metastore.MysqlStateStoreFactory;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanListDeserializer;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanListSerializer;

import static org.apache.gobblin.service.modules.orchestration.DagManagerUtils.*;


/**
 * A implementation of {@link DagStateStore} using MySQL as a backup, leverage {@link MysqlStateStore}.
 * It implements interfaces of {@link DagStateStore} but delegating responsibilities to methods provided
 * in {@link MysqlStateStore}.
 * It also implements conversion between {@link Dag<JobExecutionPlan>} to {@link State}.
 *
 * The schema of this will simply be:
 * | storeName | tableName | State |
 * where storeName represents FlowId, a combination of FlowGroup and FlowName, and tableName represents FlowExecutionId.
 * State is a pocket for serialized {@link Dag} object.
 *
 * TODO: In the DagManagerTest: change the hardcoded type of DagStateStore.
 *
 */
public class MysqlDagStateStore implements DagStateStore {

  public static final String CONFIG_PREFIX = GOBBLIN_SERVICE_PREFIX + "mysqlDagStateStore";
  public static final String DAG_KEY_IN_STATE = "dag";

  /**
   * The schema of {@link MysqlStateStore} is fixed but the columns are semantically projected into Dag's context:
   * - The 'storeName' is FlowId.
   * - The 'tableName' is
   */
  private StateStore<State> stateStore;
  private final GaasSerDe<List<JobExecutionPlan>> serDe;

  public MysqlDagStateStore(Config config, Map<URI, TopologySpec> topologySpecMap)
      throws IOException, ReflectiveOperationException {
    if (config.hasPath(CONFIG_PREFIX)) {
      config = config.getConfig(CONFIG_PREFIX).withFallback(config);
    }

    this.stateStore = (MysqlStateStoreFactory.class.newInstance()).createStateStore(config, State.class);

    JsonSerializer<List<JobExecutionPlan>> serializer = new JobExecutionPlanListSerializer();
    JsonDeserializer<List<JobExecutionPlan>> deserializer = new JobExecutionPlanListDeserializer(topologySpecMap);
    this.serDe = new GaasSerDe<>(serializer, deserializer);
  }

  @Override
  public void writeCheckpoint(Dag<JobExecutionPlan> dag) throws IOException {
    stateStore.put(generateFlowIdInString(dag), getFlowExecId(dag) + "", convertDagIntoState(dag));
  }

  @Override
  public void cleanUp(Dag<JobExecutionPlan> dag) throws IOException {
    stateStore.delete(generateFlowIdInString(dag), getFlowExecId(dag) + "");
  }

  @Override
  public List<Dag<JobExecutionPlan>> getDags() throws IOException {
    return stateStore.getAll("%").stream().map(this::convertStateObjIntoDag).collect(Collectors.toList());
  }

  /**
   * For {@link Dag} to work with {@link MysqlStateStore}, it needs to be packaged into a {@link State} object.
   * The way that it does is simply serialize the {@link Dag} first and use the key {@link #DAG_KEY_IN_STATE}
   * to be pair with it.
   *
   * The serialization step is required for readability and portability of serde lib.
   * @param dag The dag to be converted.
   * @return An {@link State} object that contains a single k-v pair for {@link Dag}.
   */
  private State convertDagIntoState(Dag<JobExecutionPlan> dag) {
    State outputState = new State();

    // Make sure the object has been serialized.
//    outputState.setProp(DAG_KEY_IN_STATE, );
    return outputState;
  }

  /**
   * Get the {@link Dag} out of a {@link State} pocket.
   */
  private Dag<JobExecutionPlan> convertStateObjIntoDag(State state) {
    return null;
  }
}
