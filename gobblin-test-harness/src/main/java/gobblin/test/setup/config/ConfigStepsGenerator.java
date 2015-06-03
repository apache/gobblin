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
import java.util.Properties;


/**
 * An interface for generating the steps associated to the config. The steps may include
 * copy of data or validating of the test. 
 * First the input to this interface is the config{@link ConfigReader#parseConfigEntry(I)} entry and the 
 * 
 * @author sveerama
 *
 * @param <C> Config for the test
 * @param <S> The steps for the test execution
 */

public interface ConfigStepsGenerator{
  /**
   *  This method will generate the list of execution steps associated to the config,
   * @param {@link ConfigReader}
   * @return List of config steps in {@link Step}
   */
  public Collection<Step> generateExecutionSteps(Properties p);
}
