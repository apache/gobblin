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

package gobblin.compaction.mapreduce.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import gobblin.compaction.mapreduce.MRCompactorJobRunner;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.AvroUtils;


/**
 * A subclass of {@link gobblin.compaction.mapreduce.MRCompactorJobRunner} that configures
 * and runs MR compaction job for Avro data.
 *
 * To dedup using entire records set compaction.use.all.attributes=true. Otherwise, a schema needs
 * to be provided by compaction.avro.key.schema.loc, based on which the dedup is performed.
 *
 * @author ziliu
 */
public class MRCompactorAvroKeyDedupJobRunner extends MRCompactorJobRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MRCompactorAvroKeyDedupJobRunner.class);
  private static final String AVRO = "avro";

  public MRCompactorAvroKeyDedupJobRunner(State jobProps, FileSystem fs) {
    super(jobProps, fs);
  }

  @Override
  protected void configureJob(Job job) throws IOException {
    super.configureJob(job);
    configureSchema(job);
  }

  private void configureSchema(Job job) throws IOException {
    Schema newestSchema = getNewestSchemaFromSource(job);
    Schema keySchema = getKeySchema(job, newestSchema);
    AvroJob.setInputKeySchema(job, newestSchema);
    AvroJob.setMapOutputKeySchema(job, keySchema);
    AvroJob.setMapOutputValueSchema(job, newestSchema);
    AvroJob.setOutputKeySchema(job, newestSchema);
  }

  /**
   * Obtain the schema used for compaction. If compaction.use.all.attributes=true, it returns newestSchema.
   * Otherwise, it reads the schema from compaction.avro.key.schema.loc.
   *
   * If it cannot read the schema specified in compaction.avro.key.schema.loc, for any reason, or if the schema
   * is not valid, it will use newestSchema.
   */
  private Schema getKeySchema(Job job, Schema newestSchema) throws IOException {
    Schema keySchema = null;
    if (useAllAttributesForCompaction()) {
      LOG.info("Using all attributes in the schema (except Map fields) for compaction");
      keySchema = AvroUtils.removeUncomparableFields(newestSchema).get();
    } else if (keySchemaFileSpecified()) {
      Path keySchemaFile = getKeySchemaFile();
      LOG.info("Using attributes specified in schema file " + keySchemaFile + " for compaction");
      try {
        keySchema = AvroUtils.parseSchemaFromFile(keySchemaFile, this.fs);
      } catch (IOException e) {
        LOG.error("Failed to parse avro schema from " + keySchemaFile
            + ", using all attributes in the schema (except Map fields) for compaction");
        keySchema = AvroUtils.removeUncomparableFields(newestSchema).get();
      }
      if (!isKeySchemaValid(keySchema, newestSchema)) {
        LOG.warn(String.format("Key schema %s is not compatible with record schema %s.", keySchema, newestSchema)
            + "Using all attributes in the schema (except Map fields) for compaction");
        keySchema = AvroUtils.removeUncomparableFields(newestSchema).get();
      }
    } else {
      LOG.info("Property " + ConfigurationKeys.COMPACTION_AVRO_KEY_SCHEMA_LOC
          + " not provided. Using all attributes in the schema (except Map fields) for compaction");
      keySchema = AvroUtils.removeUncomparableFields(newestSchema).get();
    }

    return keySchema;
  }

  /**
   * keySchema is valid if a record with newestSchema can be converted to a record with keySchema.
   */
  private boolean isKeySchemaValid(Schema keySchema, Schema newestSchema) {
    return SchemaCompatibility.checkReaderWriterCompatibility(keySchema, newestSchema).getType()
        .equals(SchemaCompatibilityType.COMPATIBLE);
  }

  private Schema getNewestSchemaFromSource(Job job) throws IOException {
    Path[] sourceDirs = FileInputFormat.getInputPaths(job);

    List<FileStatus> files = new ArrayList<FileStatus>();

    for (Path sourceDir : sourceDirs) {
      files.addAll(Arrays.asList(this.fs.listStatus(sourceDir)));
    }

    Collections.sort(files, new LastModifiedDescComparator());

    for (FileStatus file : files) {
      Schema schema = getNewestSchemaFromSource(file.getPath());
      if (schema != null) {
        return schema;
      }
    }
    return null;
  }

  @SuppressWarnings("deprecation")
  private Schema getNewestSchemaFromSource(Path sourceDir) throws IOException {
    FileStatus[] files = fs.listStatus(sourceDir);
    Arrays.sort(files, new LastModifiedDescComparator());
    for (FileStatus status : files) {
      if (status.isDir()) {
        Schema schema = getNewestSchemaFromSource(status.getPath());
        if (schema != null)
          return schema;
      } else if (FilenameUtils.isExtension(status.getPath().getName(), AVRO)) {
        return AvroUtils.getSchemaFromDataFile(status.getPath(), this.fs);
      }
    }
    return null;
  }

  private boolean useAllAttributesForCompaction() {
    return this.jobProps.getPropAsBoolean(ConfigurationKeys.COMPACTION_USE_ALL_ATTRIBUTES,
        ConfigurationKeys.DEFAULT_COMPACTION_USE_ALL_ATTRIBUTES);
  }

  private boolean keySchemaFileSpecified() {
    return this.jobProps.contains(ConfigurationKeys.COMPACTION_AVRO_KEY_SCHEMA_LOC);
  }

  private Path getKeySchemaFile() {
    return new Path(this.jobProps.getProp(ConfigurationKeys.COMPACTION_AVRO_KEY_SCHEMA_LOC));
  }

  @Override
  protected void setInputFormatClass(Job job) {
    job.setInputFormatClass(AvroKeyRecursiveCombineFileInputFormat.class);
  }

  @Override
  protected void setMapperClass(Job job) {
    job.setMapperClass(AvroKeyMapper.class);
  }

  @Override
  protected void setMapOutputKeyClass(Job job) {
    job.setMapOutputKeyClass(AvroKey.class);
  }

  @Override
  protected void setMapOutputValueClass(Job job) {
    job.setMapOutputValueClass(AvroValue.class);
  }

  @Override
  protected void setOutputFormatClass(Job job) {
    job.setOutputFormatClass(AvroKeyCompactorOutputFormat.class);
  }

  @Override
  protected void setReducerClass(Job job) {
    job.setReducerClass(AvroKeyDedupReducer.class);
  }

  @Override
  protected void setOutputKeyClass(Job job) {
    job.setOutputKeyClass(AvroKey.class);
  }

  @Override
  protected void setOutputValueClass(Job job) {
    job.setOutputValueClass(NullWritable.class);
  }

  /**
   * A Comparator for reverse order comparison of modification time of two FileStatus.
   */
  private static class LastModifiedDescComparator implements Comparator<FileStatus> {

    @Override
    public int compare(FileStatus fs1, FileStatus fs2) {
      if (fs2.getModificationTime() < fs1.getModificationTime())
        return -1;
      else if (fs2.getModificationTime() > fs1.getModificationTime())
        return 1;
      else
        return 0;
    }
  }

}
