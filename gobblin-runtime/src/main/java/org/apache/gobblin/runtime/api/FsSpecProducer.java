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
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.job_spec.AvroJobSpec;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.CompletedFuture;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.HadoopUtils;


/**
 * An implementation of {@link SpecProducer} that produces {@link JobSpec}s to the {@value FsSpecConsumer#SPEC_PATH_KEY}
 * for consumption by the {@link FsSpecConsumer}.
 *
 * The pair {@link FsSpecProducer} and {@link FsSpecConsumer} assumes serialization format as Avro. More specifically,
 * {@link JobSpec}s will be serialized as ".avro" file by {@link FsSpecProducer} and {@link FsSpecConsumer} filtered
 * all files without proper postfix to avoid loading corrupted {@link JobSpec}s that could possibly existed due to
 * ungraceful exits of the application or weak file system semantics.
 *
 */
@Slf4j
public class FsSpecProducer implements SpecProducer<Spec> {
  private Path specConsumerPath;
  private FileSystem fs;

  public FsSpecProducer(Config config) {
    this(null, config);
  }

  public FsSpecProducer(@Nullable FileSystem fs, Config config) {
    String specConsumerDir = ConfigUtils.getString(config, FsSpecConsumer.SPEC_PATH_KEY, "");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(specConsumerDir), "Missing argument: " + FsSpecConsumer.SPEC_PATH_KEY);
    this.specConsumerPath = new Path(specConsumerDir);
    try {
      this.fs = (fs == null) ? FileSystem.get(new Configuration()) : fs;
    } catch (IOException e) {
      throw new RuntimeException(e);
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
      try {
        AvroJobSpec avroJobSpec = convertToAvroJobSpec((JobSpec) spec, verb);
        writeAvroJobSpec(avroJobSpec);
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
   * @param headers*/
  @Override
  public Future<?> deleteSpec(URI deletedSpecURI, Properties headers) {
    AvroJobSpec avroJobSpec = AvroJobSpec.newBuilder().setUri(deletedSpecURI.toString())
        .setMetadata(ImmutableMap.of(SpecExecutor.VERB_KEY, SpecExecutor.Verb.DELETE.name()))
        .setProperties(Maps.fromProperties(headers)).build();
    try {
      writeAvroJobSpec(avroJobSpec);
      return new CompletedFuture<>(Boolean.TRUE, null);
    } catch (IOException e) {
      log.error("Exception encountered when writing DELETE spec");
      return new CompletedFuture<>(Boolean.TRUE, e);
    }
  }

  /** List all {@link Spec} being executed on {@link org.apache.gobblin.runtime.api.SpecExecutor}. */
  @Override
  public Future<? extends List<Spec>> listSpecs() {
    throw new UnsupportedOperationException();
  }

  private AvroJobSpec convertToAvroJobSpec(JobSpec jobSpec, SpecExecutor.Verb verb) {
    return AvroJobSpec.newBuilder().
        setUri(jobSpec.getUri().toString()).
        setProperties(Maps.fromProperties(jobSpec.getConfigAsProperties())).
        setTemplateUri("FS:///").
        setDescription(jobSpec.getDescription()).
        setVersion(jobSpec.getVersion()).
        setMetadata(ImmutableMap.of(SpecExecutor.VERB_KEY, verb.name())).build();
  }

  private void writeAvroJobSpec(AvroJobSpec jobSpec) throws IOException {
    DatumWriter<AvroJobSpec> datumWriter = new SpecificDatumWriter<>(AvroJobSpec.SCHEMA$);
    DataFileWriter<AvroJobSpec> dataFileWriter = new DataFileWriter<>(datumWriter);

    Path jobSpecPath = new Path(this.specConsumerPath, annotateSpecFileName(jobSpec.getUri()));

    //Write the new JobSpec to a temporary path first.
    Path tmpDir = new Path(this.specConsumerPath, UUID.randomUUID().toString());
    if (!fs.exists(tmpDir)) {
      fs.mkdirs(tmpDir);
    }

    Path tmpJobSpecPath = new Path(tmpDir, jobSpec.getUri());

    OutputStream out = fs.create(tmpJobSpecPath);

    dataFileWriter.create(AvroJobSpec.SCHEMA$, out);
    dataFileWriter.append(jobSpec);
    dataFileWriter.close();

    //Rename the JobSpec from temporary to final location.
    HadoopUtils.renamePath(fs, tmpJobSpecPath, jobSpecPath, true);

    //Delete the temporary path once the jobspec has been moved to its final publish location.
    log.info("Deleting {}", tmpJobSpecPath.getParent().toString());
    fs.delete(tmpJobSpecPath.getParent(), true);
  }

  private String annotateSpecFileName(String rawName) {
    return rawName + AvroUtils.AVRO_SUFFIX;
  }
}