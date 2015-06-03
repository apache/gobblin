package gobblin.test.setup.config;

import gobblin.test.execution.TestHarnessExecutor;

/**
 * An interface for parsing the config data for test harness. 
 * This interface is allows to parse and validate the config setting
 * 
 * @author sveerama
 *
 * @param <I> Input folder where the config (JSON/YAML) is stored
 * @param <C> The reader of the input for the config
 */

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
public interface ConfigReader<I, C> extends ConfigStepsGenerator<C>,TestHarnessExecutor<C>{

  /**
   *  This method will parse the config based on the input provided
   *
   * @param inputFolder
   * @return config
   *
   */

  public C parseConfigEntry(I testConfigFolder);

  /**
   *  This method will validate the config implementer, this allows to customize the config validation
   *   for each type of config 
   *
   * @param <C> config
   * @return Boolean
   *
   */
  
  
  public Boolean validateConfigEntry(C config);
}
