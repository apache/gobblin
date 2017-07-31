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
package org.apache.gobblin.test.setup.config;


/**
 * This interface is to define individual steps associated to a config entry, this implements the Operator interface.
 * Essentially one step can have many operators and each operator has an execution
 *
 * @author sveerama
 *
 */

public interface Step {

  /**
   * This method will execute the current step which in turn will execute list of operators
   *
   */
  public boolean execute() throws Exception;

  /**
   * Get the name of the step in the series of steps for information
   * @return step name
   */
  public String getStepName();
}
