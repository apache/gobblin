/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.commit;

import java.io.IOException;

import gobblin.annotation.Alpha;


/**
 * A step during committing a dataset that should be executed atomically with other steps under exactly-once semantics.
 * An example is publishing the data files of a dataset, which should be executed atomically with persisting the
 * dataset state in order to avoid pulling duplicate data.
 *
 * @author ziliu
 */
@Alpha
public interface CommitStep {

  /**
   * Determine whether the commit step has been completed.
   */
  public boolean isCompleted() throws IOException;

  /**
   * Execute the commit step.
   */
  public void execute() throws IOException;
}
