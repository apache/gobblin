/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.dataset.config;

/**
 * Since the configuration will be loaded into memory only once, the loaded configuration version is determined once loaded.
 * 
 * This exception will signal the configuration miss match between input query version and the configuration version loaded in memory.
 * @author mitu
 *
 */
public class ConfigVersionMissMatchException extends IllegalArgumentException {

  /**
   * 
   */
  private static final long serialVersionUID = 2824330818150599343L;

  public ConfigVersionMissMatchException(String message) {
    super(message);
  }

}
