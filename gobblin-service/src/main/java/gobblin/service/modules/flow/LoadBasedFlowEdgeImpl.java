package gobblin.service.modules.flow;

import java.util.Properties;

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
@Data
public class LoadBasedFlowEdgeImpl extends DefaultWeightedEdge implements FlowEdge {
  private ServiceNode sourceNode;
  private ServiceNode targetNode;
  private final FlowEdgeProps _flowEdgeProps;
  private SpecExecutor specExecutorInstance;

  /**
   * {@link #load} and {@link #edgeSafety}'s value comes from {@link FlowEdgeProps} initially
   * and can be overrided by {@link #setEdgeSafety(boolean)} and {@link #setEdgeLoad(double)} method afterwards.
   */
  private boolean edgeSafety;
  private double load;

  public LoadBasedFlowEdgeImpl(ServiceNode sourceNode, ServiceNode targetNode,
      FlowEdgeProps flowEdgeProps, SpecExecutor specExecutorInstance) {
    this.sourceNode = sourceNode ;
    this.targetNode = targetNode ;
    this._flowEdgeProps = flowEdgeProps;
    this.specExecutorInstance = specExecutorInstance;

    this.edgeSafety = flowEdgeProps.getInitialEdgeSafety();
    this.load = flowEdgeProps.getInitialEdgeLoad();
  }

  public LoadBasedFlowEdgeImpl(ServiceNode sourceNode, ServiceNode targetName,
      SpecExecutor specExecutorInstance){
    this(sourceNode, targetName, new FlowEdgeProps(specExecutorInstance.getAttrs()), specExecutorInstance);
  }

  /**
   * Specific to this implementation that deals with edge load.
   */
  public double getEdgeLoad(){
    return this.load;
  }

  public void setEdgeLoad(double load){
    this.load = load;
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


  protected double getWeight()
  {
    return getEdgeLoad();
  }
}
