package gobblin.service.modules.flow;

import gobblin.util.ConfigUtils;
import gobblin.util.PropertiesUtils;
import java.util.Properties;

import lombok.Getter;
import org.jgrapht.graph.DefaultWeightedEdge;

import gobblin.annotation.Alpha;
import gobblin.runtime.api.FlowEdge;
import gobblin.runtime.api.ServiceNode;
import gobblin.runtime.api.SpecExecutor;

import lombok.Data;

/**
 * A base implementation of a flowEdge in the weight multi-edge graph.
 * For a weightedMultiGraph there could be multiple edges between two vertices.
 * A triplet of <SourceNode, targetNode, specExecutorInstance> determines one edge.
 * Each edge has a {@FlowEdgeProp} which is used to evaluate a edge.
 *
 * The {@link LoadBasedFlowEdgeImpl} exposes additional interface to get/set load to edge directly instead of thru.
 * {@link FlowEdgeProps}
 */
@Alpha
public class LoadBasedFlowEdgeImpl extends DefaultWeightedEdge implements FlowEdge {
  @Getter
  private ServiceNode sourceNode;
  @Getter
  private ServiceNode targetNode;
  @Getter
  private SpecExecutor specExecutorInstance;

  private final FlowEdgeProps _flowEdgeProps;

  /**
   * {@link #load} and {@link #edgeSafety}'s value comes from {@link FlowEdgeProps} initially
   * and can be overrided by {@link #setEdgeSafety(boolean)} and
   * {@link org.jgrapht.graph.DirectedWeightedMultigraph#setEdgeWeight(Object, double)} method afterwards.
   */
  private boolean edgeSafety;

  /**
   * Should make this value consistent with value of {@link DefaultWeightedEdge}'s {@link #getWeight()}
   * In this implementation it basically behaves like a cache layer of {@link #getWeight()}
   * The purpose for keep this field is:
   * - {@link #getWeight()} method is protected.
   * - Keep the flexibility to implement more complicated load.
   *
   * Should not expose set method since weight setting will only be accessible
   * by {@link org.jgrapht.graph.DirectedWeightedMultigraph#setEdgeWeight}
   */
  private double load;

  public LoadBasedFlowEdgeImpl(ServiceNode sourceNode, ServiceNode targetNode,
      FlowEdgeProps flowEdgeProps, SpecExecutor specExecutorInstance) {
    this.sourceNode = sourceNode ;
    this.targetNode = targetNode ;
    this._flowEdgeProps = flowEdgeProps;
    this.specExecutorInstance = specExecutorInstance;

    this.edgeSafety = flowEdgeProps.getInitialEdgeSafety();
    this.load = getWeight();
  }

  public LoadBasedFlowEdgeImpl(ServiceNode sourceNode, ServiceNode targetNode,
      SpecExecutor specExecutor){
    this(sourceNode, targetNode, new FlowEdgeProps(ConfigUtils.configToProperties(specExecutor.getAttrs())),
        specExecutor);
  }

  /**
   *
   * @return
   */
  public double getEdgeLoad(){
    this.load = getWeight();
    return this.load;
  }


  @Override
  public String getEdgeIdentity(){
    return this.calculateEdgeIdentity(this.sourceNode, this.targetNode, this.specExecutorInstance);
  }

  @Override
  /**
   * Since {@link #_flowEdgeProps} is supposed to be read-only once instantiated, set method here
   * will overwrite the value in {@link #edgeSafety}.
   */
  public void setEdgeSafety(boolean safety) {
    this.edgeSafety = safety;
  }

  @Override
  public boolean isEdgeSafe() {
    return this.edgeSafety;
  }

  @Override
  public Properties getEdgeProperties() {
    return this._flowEdgeProps.getProperties();
  }

  @Override
  /**
   * Naive implementation for here.
   */
  public boolean isEdgeValid() {
    return this.edgeSafety;
  }

  public static String calculateEdgeIdentity(ServiceNode sourceNode, ServiceNode targetNode, SpecExecutor specExecutorInstance){
    return sourceNode.getNodeName() + "-" + specExecutorInstance.getUri() + "-" + targetNode.getNodeName();
  }

  /**
   * Recall that we need a triplet to uniquely define a {@link FlowEdge}:
   * - {@link ServiceNode} sourceNode
   * - {@link ServiceNode} targetNode
   * - {@link SpecExecutor} SpecExecutor
   *
   * We DO NOT distinguish between two edges by other props like {@link #load}(weight),
   * as the load should be an attribute of an edge.
   * These are IntelliJ-generated methods for equals and hashCode().
   *
   * @param o The object that being compared
   * @return If two {@link LoadBasedFlowEdgeImpl} are equivalent.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LoadBasedFlowEdgeImpl that = (LoadBasedFlowEdgeImpl) o;

    if (!sourceNode.equals(that.sourceNode)) {
      return false;
    }
    if (!targetNode.equals(that.targetNode)) {
      return false;
    }
    return specExecutorInstance.equals(that.specExecutorInstance);
  }

  @Override
  public int hashCode() {
    int result = sourceNode.hashCode();
    result = 31 * result + targetNode.hashCode();
    result = 31 * result + specExecutorInstance.hashCode();
    return result;
  }
}
