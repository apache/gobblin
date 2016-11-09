/*
 * Copyright (C) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy.hive;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

/** Denotes that the desired target table location in Hive does not match the existing target table location */
public class HiveTableLocationNotMatchException extends IOException {

  private static final long serialVersionUID = 1L;

  private final Path desiredTargetTableLocation;
  private final Path existingTargetTableLocation;

  public HiveTableLocationNotMatchException(Path desired, Path existing) {
    super(String.format("Desired target location %s and already registered target location %s do not agree.",
              desired, existing));
    this.desiredTargetTableLocation = desired;
    this.existingTargetTableLocation = existing;
  }

  public Path getDesiredTargetTableLocation(){
    return this.desiredTargetTableLocation;
  }

  public Path getExistingTargetTableLocation() {
    return this.existingTargetTableLocation;
  }
}
