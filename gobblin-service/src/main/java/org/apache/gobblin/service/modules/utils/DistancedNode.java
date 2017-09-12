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

package org.apache.gobblin.service.modules.utils;
import org.apache.gobblin.runtime.api.ServiceNode;

/**
 * This is a helping class(Basically a wrapper) for Shortest path finding process
 * to keep the shortest distance from source to an arbitrary node.
 */
public class DistancedNode<T extends ServiceNode> {

  /**
   * The distance between {@link this} node to the src node in the shortest-distance finding problem.
   */
  private double distToSrc;

  private T _serviceNode;


  /**
   * Max_Value represents no-connection.
   */
  public DistancedNode(T _serviceNode){
    this(_serviceNode, Double.MAX_VALUE);
  }

  public DistancedNode(T _serviceNode, double dist){
    this._serviceNode = _serviceNode;
    this.distToSrc = dist;
  }

  public double getDistToSrc(){
    return this.distToSrc;
  }

  public void setDistToSrc(double distToSrc){
    this.distToSrc = distToSrc;
  }

  public T getNode(){
    return this._serviceNode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DistancedNode<?> that = (DistancedNode<?>) o;

    return _serviceNode.equals(that._serviceNode);
  }

  @Override
  public int hashCode() {
    return _serviceNode.hashCode();
  }
}