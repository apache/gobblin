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

package gobblin.hive;

import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import gobblin.commit.CommitStep;
import gobblin.hive.spec.HiveSpec;


/**
 * {@link CommitStep} to perform a Hive registration.
 */
@Slf4j
@AllArgsConstructor
public class HiveRegisterStep implements CommitStep {

  private final Optional<String> metastoreURI;
  private final HiveSpec hiveSpec;
  private final HiveRegProps props;

  @Override
  public boolean isCompleted() throws IOException {
    // TODO: this is complicated due to preactivities, postactivities, etc. but unnecessary for now because exactly once
    // is not enabled.
    return false;
  }

  @Override
  public void execute() throws IOException {
    HiveRegister hiveRegister = HiveRegister.get(this.props, this.metastoreURI);
    log.info("Registering Hive Spec " + this.hiveSpec);
    ListenableFuture<Void> future = hiveRegister.register(this.hiveSpec);
    try {
      future.get();
    } catch (InterruptedException | ExecutionException ie) {
      throw new IOException("Hive registration was interrupted.", ie);
    }
  }

  @Override
  public String toString() {
    String table = this.hiveSpec.getTable().getDbName() + "." + this.hiveSpec.getTable().getTableName();
    String partitionInfo = this.hiveSpec.getPartition().isPresent()
        ? " partition " + Arrays.toString(this.hiveSpec.getPartition().get().getValues().toArray())
        : "";
    String location = this.hiveSpec.getPartition().isPresent()
        ? this.hiveSpec.getPartition().get().getLocation().get()
        : this.hiveSpec.getTable().getLocation().get();
    return String.format("Register %s%s with location %s in Hive metastore %s.", table, partitionInfo, location,
        this.metastoreURI.isPresent() ? this.metastoreURI.get() : "default");
  }
}
