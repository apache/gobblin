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

package org.apache.gobblin.hive;

import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.hive.spec.HiveSpec;


/**
 * {@link CommitStep} to perform a Hive registration.
 */
@Slf4j
@AllArgsConstructor
public class HiveRegisterStep implements CommitStep {

  public HiveRegisterStep(Optional<String> metastoreURI, HiveSpec hiveSpec, HiveRegProps props) {
    this(metastoreURI, hiveSpec, props, true);
  }

  private final Optional<String> metastoreURI;
  private final HiveSpec hiveSpec;
  private final HiveRegProps props;
  private final boolean verifyBeforeRegistering;

  @Override
  public boolean isCompleted() throws IOException {
    // TODO: this is complicated due to preactivities, postactivities, etc. but unnecessary for now because exactly once
    // is not enabled.
    return false;
  }

  @Override
  public void execute() throws IOException {

    if (this.verifyBeforeRegistering) {
      if (!this.hiveSpec.getTable().getLocation().isPresent()) {
        throw getException("Table does not have a location parameter.");
      }
      Path tablePath = new Path(this.hiveSpec.getTable().getLocation().get());

      FileSystem fs = this.hiveSpec.getPath().getFileSystem(new Configuration());
      if (!fs.exists(tablePath)) {
        throw getException(String.format("Table location %s does not exist.", tablePath));
      }

      if (this.hiveSpec.getPartition().isPresent()) {

        if (!this.hiveSpec.getPartition().get().getLocation().isPresent()) {
          throw getException("Partition does not have a location parameter.");
        }
        Path partitionPath = new Path(this.hiveSpec.getPartition().get().getLocation().get());
        if (!fs.exists(this.hiveSpec.getPath())) {
          throw getException(String.format("Partition location %s does not exist.", partitionPath));
        }
      }
    }

    try (HiveRegister hiveRegister = HiveRegister.get(this.props, this.metastoreURI)) {
      log.info("Registering Hive Spec " + this.hiveSpec);
      ListenableFuture<Void> future = hiveRegister.register(this.hiveSpec);
      future.get();
    } catch (InterruptedException | ExecutionException ie) {
      throw new IOException("Hive registration was interrupted.", ie);
    }
  }

  private IOException getException(String message) {
    return new IOException(
        String.format("Failed to register Hive Spec %s. %s", this.hiveSpec, message)
    );
  }

  @Override
  public String toString() {
    String table = this.hiveSpec.getTable().getDbName() + "." + this.hiveSpec.getTable().getTableName();
    String partitionInfo = this.hiveSpec.getPartition().isPresent()
        ? " partition " + Arrays.toString(this.hiveSpec.getPartition().get().getValues().toArray()) : "";
    String location = this.hiveSpec.getPartition().isPresent() ? this.hiveSpec.getPartition().get().getLocation().get()
        : this.hiveSpec.getTable().getLocation().get();
    return String.format("Register %s%s with location %s in Hive metastore %s.", table, partitionInfo, location,
        this.metastoreURI.isPresent() ? this.metastoreURI.get() : "default");
  }
}
