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

package org.apache.gobblin.hive.spec.activity;

import java.io.IOException;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.hive.HiveRegister;


/**
 * Execute a task for a given input.
 */
@Alpha
public interface Activity {

  /**
   * Execute a task given a {@link HiveRegister}.
   *
   * @return true if the execution is successful; false otherwise.
   */
  public boolean execute(HiveRegister register) throws IOException;

}
