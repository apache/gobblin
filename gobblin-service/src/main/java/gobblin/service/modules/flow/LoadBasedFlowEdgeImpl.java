package gobblin.service.modules.flow;

import gobblin.annotation.Alpha;
import gobblin.runtime.api.FlowEdge;
import gobblin.runtime.api.ServiceNode;
import gobblin.runtime.api.FlowEdgeMetric;

import gobblin.runtime.api.SpecExecutorInstanceProducer;

import lombok.Data;
import org.jgrapht.graph.DefaultWeightedEdge;


/**
 * A base implementation of a flowEdge in the weight multi-edge graph.
 * For a weightedGraph there could be multiple edges between two vertices.
 * A triplet of <SourceNode, targetNode, specExecutorInstanceProducer> determines one edge.
 * Each edge has a flowEdgeMetric which is to evaluate a edge when considering which edge to pick,
 * given a pair of source and destination.
 *
 * The {@link LoadBasedFlowEdgeImpl} exposes additional interface to get/set load to edge directly instead of thru.
 * {@link BaseFlowEdgeMetricImpl}
 */
@Alpha
@Data
public class LoadBasedFlowEdgeImpl extends DefaultWeightedEdge implements FlowEdge {
  private ServiceNode sourceNode;
  private ServiceNode targetNode;
  private FlowEdgeMetric flowEdgeMetric;
  private SpecExecutorInstanceProducer specExecutorInstanceProducer;

  public LoadBasedFlowEdgeImpl(ServiceNode sourceNode, ServiceNode targetNode,
      FlowEdgeMetric flowEdgeMetric, SpecExecutorInstanceProducer specExecutorInstanceProducer) {
    this.sourceNode = sourceNode ;
    this.targetNode = targetNode ;
    this.flowEdgeMetric = flowEdgeMetric ;
    this.specExecutorInstanceProducer = specExecutorInstanceProducer;
  }

  public LoadBasedFlowEdgeImpl(ServiceNode sourceNode, ServiceNode targetName,
      SpecExecutorInstanceProducer specExecutorInstanceProducer){
    this(sourceNode, targetName, new BaseFlowEdgeMetricImpl(1.0), specExecutorInstanceProducer);
  }

  @Override
  public String getEdgeIdentity(){
    return this.sourceNode.getNodeName() + "-"
        + specExecutorInstanceProducer.getUri() + "-"
        + this.targetNode.getNodeName();
  }

  public double getEdgeLoad(){
    return this.flowEdgeMetric.getFlowEdgeLoad();
  }

  public void setEdgeLoad(double load){
    this.flowEdgeMetric.setFlowEdgeLoad(load);
  }

  protected double getWeight()
  {
    return getEdgeLoad();
  }
}
