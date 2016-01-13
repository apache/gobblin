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

package gobblin.yarn;

/**
 * An enumeration of Helix message sub types.
 *
 * @author Yinan Li
 */
public enum HelixMessageSubTypes {

  /**
   * This type is for messages sent when the {@link GobblinApplicationMaster} is to be shutdown.
   */
  APPLICATION_MASTER_SHUTDOWN,

  /**
   * This type is for messages sent when the {@link GobblinWorkUnitRunner}s are to be shutdown.
   */
  WORK_UNIT_RUNNER_SHUTDOWN,

  /**
   * This type is for messages sent when the file storing the delegation token has been updated.
   */
  TOKEN_FILE_UPDATED
}
