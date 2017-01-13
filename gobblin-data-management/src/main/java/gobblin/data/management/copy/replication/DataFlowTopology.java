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

package gobblin.data.management.copy.replication;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import lombok.Data;
import lombok.AllArgsConstructor;;

/**
 * Class to represent the data flow topology from copy source to copy destinations. Each {@link DataFlowTopology} contains
 * a list of {@link DataFlowTopology.DataFlowPath}s
 *
 *
 */

@Data
public class DataFlowTopology {

  private List<DataFlowPath> dataFlowPaths = new ArrayList<>();

  public void addDataFlowPath(DataFlowPath p){
    this.dataFlowPaths.add(p);
  }

  @Override
  public String toString(){
    Function<DataFlowPath, String> func =
        new Function<DataFlowPath, String>() {
          @Override
          public String apply(DataFlowPath t) {
            return t.toString();
          }
        };

    return Objects.toStringHelper(this.getClass())
        .add("dataFlows:", Joiner.on(",").join(Lists.transform(this.dataFlowPaths, func))).toString();
  }

  @Data
  @AllArgsConstructor
  public static class DataFlowPath{
    private List<CopyRoute> copyRoutes;

    @Override
    public String toString(){
      Function<CopyRoute, String> func =
          new Function<CopyRoute, String>() {
            @Override
            public String apply(CopyRoute t) {
              return t.toString();
            }
          };

      return Objects.toStringHelper(this.getClass())
          .add("copyPairs:", Joiner.on(",").join(Lists.transform(this.copyRoutes, func))).toString();
    }
  }
}
