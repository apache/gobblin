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
package gobblin.test.execution.operator;

/**
 * An interface for defining the operator , the operator could be a copy of file or converting a file from one format to another. These operators are used for the setup phase of the test
 *
 * @author sveerama
 */

public interface SetupOperator {
  /**
   * This method is invoked to execute an operator. The operator will have an associated execution process.
   * @return the success of execution for the operator
   */
  public boolean executeOperator() throws Exception;

}
