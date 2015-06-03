/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.test.setup.config;

import java.util.Collection;

import gobblin.test.execution.operator.SetupOperator;


/**
 * This interface is to define individual steps associated to a config entry, this implements the Operator interface.
 * Essentially one step can have many operators and each operator has an execution
 * 
 * @author sveerama
 *
 */

public interface Step extends SetupOperator {

  /**
   * This method is used to generate step level operator(s).
   * The Operator is a sub-level task associated to the step
   * 
   */
  public Collection<SetupOperator> getSetupOperators();

  /**
   * This method will execute the current step which in turn will execute list of operators
   *
   */
  public Boolean execute();
}
