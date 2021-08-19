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
package org.apache.gobblin.runtime.api;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.job_spec.AvroJobSpec;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.CompletedFuture;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.filters.AndPathFilter;
import org.apache.gobblin.util.filters.HiddenFilter;


@Slf4j
public class FsSpecConsumer implements SpecConsumer<Spec> {
  public static final String SPEC_PATH_KEY = "gobblin.cluster.specConsumer.path";

  private final Path specDirPath;
  private final FileSystem fs;
  private Map<URI, Path> specToPathMap = new HashMap<>();


  public FsSpecConsumer(Config config) {
    this(null, config);
  }

  public FsSpecConsumer(@Nullable FileSystem fs, Config config) {
    this.specDirPath = new Path(config.getString(SPEC_PATH_KEY));
    try {
      this.fs = (fs == null) ? FileSystem.get(new Configuration()) : fs;
      if (!this.fs.exists(specDirPath)) {
        this.fs.mkdirs(specDirPath);
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to detect spec directory file system: " + e, e);
    }
  }

  /** List of newly changed {@link Spec}s for execution on {@link SpecExecutor}.
   * The {@link Spec}s are returned in the increasing order of their modification times.
   */
  @Override
  public Future<? extends List<Pair<SpecExecutor.Verb, Spec>>> changedSpecs() {
    List<Pair<SpecExecutor.Verb, Spec>> specList = new ArrayList<>();
    FileStatus[] fileStatuses;
    try {
      fileStatuses = this.fs.listStatus(this.specDirPath,
          new AndPathFilter(new HiddenFilter(), new AvroUtils.AvroPathFilter()));
    } catch (IOException e) {
      log.error("Error when listing files at path: {}", this.specDirPath.toString(), e);
      return null;
    }
    log.info("Found {} files at path {}", fileStatuses.length, this.specDirPath.toString());

    //Sort the {@link JobSpec}s in increasing order of their modification times.
    //This is done so that the {JobSpec}s can be handled in FIFO order by the
    //JobConfigurationManager and eventually, the GobblinHelixJobScheduler.
    Arrays.sort(fileStatuses, Comparator.comparingLong(FileStatus::getModificationTime));

    for (FileStatus fileStatus : fileStatuses) {
      DataFileReader<AvroJobSpec> dataFileReader;
      try {
        dataFileReader = new DataFileReader<>(new FsInput(fileStatus.getPath(), this.fs.getConf()), new SpecificDatumReader<>());
      } catch (IOException e) {
        log.error("Error creating DataFileReader for: {}", fileStatus.getPath().toString(), e);
        continue;
      }

      AvroJobSpec avroJobSpec = null;
      while (dataFileReader.hasNext()) {
        avroJobSpec = dataFileReader.next();
        break;
      }

      if (avroJobSpec != null) {
        JobSpec.Builder jobSpecBuilder = new JobSpec.Builder(avroJobSpec.getUri());
        Properties props = new Properties();
        props.putAll(avroJobSpec.getProperties());
        jobSpecBuilder.withJobCatalogURI(avroJobSpec.getUri())
            .withVersion(avroJobSpec.getVersion())
            .withDescription(avroJobSpec.getDescription())
            .withConfigAsProperties(props)
            .withConfig(ConfigUtils.propertiesToConfig(props));

        try {
          if (!avroJobSpec.getTemplateUri().isEmpty()) {
            jobSpecBuilder.withTemplate(new URI(avroJobSpec.getTemplateUri()));
          }
        } catch (URISyntaxException u) {
          log.error("Error building a job spec: ", u);
          continue;
        }

        String verbName = avroJobSpec.getMetadata().get(SpecExecutor.VERB_KEY);
        SpecExecutor.Verb verb = SpecExecutor.Verb.valueOf(verbName);

        JobSpec jobSpec = jobSpecBuilder.build();
        log.debug("Successfully built jobspec: {}", jobSpec.getUri().toString());
        specList.add(new ImmutablePair<SpecExecutor.Verb, Spec>(verb, jobSpec));
        this.specToPathMap.put(jobSpec.getUri(), fileStatus.getPath());
      }
    }
    return new CompletedFuture<>(specList, null);
  }


  @Override
  public void commit(Spec spec) throws IOException {
    Path path = this.specToPathMap.get(spec.getUri());
    if (path != null) {
      log.debug("Calling delete on path: {}", path.toString());
      this.fs.delete(path, false);
    } else {
      log.error("No path found for job: {}", spec.getUri().toString());
    }
  }
}
