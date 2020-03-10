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

package org.apache.gobblin.runtime.spec_executorInstance;
import com.typesafe.config.Config;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.util.CompletedFuture;


/**
 * An implementation of {@link SpecProducer} that produces {@link JobSpec}s to the {@value #LOCAL_FS_PRODUCER_PATH_KEY}
 */
@Slf4j
public class LocalFsSpecProducer implements SpecProducer<Spec> {
  private String specProducerPath;
  public static final String LOCAL_FS_PRODUCER_PATH_KEY = "localFsSpecProducer.dir";

  public LocalFsSpecProducer(Config config) {
    this.specProducerPath = config.getString(LOCAL_FS_PRODUCER_PATH_KEY);
    File parentDir = new File(specProducerPath);
    if (!parentDir.exists()) {
      if (parentDir.mkdirs()) {
        log.info("Creating directory path at {}", this.specProducerPath);
      } else {
        throw new RuntimeException(String.format("Unable to create folder to write specs to at %s", this.specProducerPath));
      }
    }
  }

  /** Add a {@link Spec} for execution on {@link org.apache.gobblin.runtime.api.SpecExecutor}.
   * @param addedSpec*/
  @Override
  public Future<?> addSpec(Spec addedSpec) {
    return writeSpec(addedSpec, SpecExecutor.Verb.ADD);
  }

  /** Update a {@link Spec} being executed on {@link org.apache.gobblin.runtime.api.SpecExecutor}.
   * @param updatedSpec*/
  @Override
  public Future<?> updateSpec(Spec updatedSpec) {
    return writeSpec(updatedSpec, SpecExecutor.Verb.UPDATE);
  }

  private Future<?> writeSpec(Spec spec, SpecExecutor.Verb verb) {
    if (spec instanceof JobSpec) {
      // format the JobSpec to have file of <flowGroup>_<flowName>.job
      String flowExecutionId = ((JobSpec) spec).getConfigAsProperties().getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
      String jobFileName = getJobFileName(spec.getUri(), flowExecutionId);
      try (
        FileOutputStream fStream = new FileOutputStream(this.specProducerPath + File.separatorChar + jobFileName);
      ) {
        ((JobSpec) spec).getConfigAsProperties().store(fStream, null);
        log.info("Writing job {} to {}", jobFileName, this.specProducerPath);
        return new CompletedFuture<>(Boolean.TRUE, null);
      } catch (IOException e) {
        log.error("Exception encountered when adding Spec {}", spec);
        return new CompletedFuture<>(Boolean.TRUE, e);
      }
    } else {
      throw new RuntimeException("Unsupported spec type " + spec.getClass());
    }
  }

  /** Delete a {@link Spec} being executed on {@link org.apache.gobblin.runtime.api.SpecExecutor}.
   * @param deletedSpecURI
   * @param headers
   */
  @Override
  public Future<?> deleteSpec(URI deletedSpecURI, Properties headers) {
    String prefix = String.join("_", deletedSpecURI.getPath().split("/"));
    // delete all of the jobs related to the spec
    File dir = new File(this.specProducerPath);
    File[] foundFiles = dir.listFiles((File file, String name) -> {
        // only delete the jobs in progress
        return name.startsWith(prefix) && name.endsWith(".job");
    });

    for (int i = 0; i < foundFiles.length; i++) {
        Boolean didDelete = foundFiles[i].delete();
        if (!didDelete) {
          return new CompletedFuture<>(Boolean.TRUE, new RuntimeException(String.format("Failed to delete file with uri %s", deletedSpecURI)));
        }
    }

    return new CompletedFuture<>(Boolean.TRUE, null);
  }

  /** List all {@link Spec} being executed on {@link org.apache.gobblin.runtime.api.SpecExecutor}. */
  @Override
  public Future<? extends List<Spec>> listSpecs() {
    throw new UnsupportedOperationException();
  }

  public static String getJobFileName(URI specUri, String flowExecutionId) {
    String[] uriTokens = specUri.getPath().split("/");
    return String.join("_", uriTokens) + "_" + flowExecutionId + ".job";
  }

}