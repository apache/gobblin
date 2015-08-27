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
import java.util.Collection;
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

import com.google.common.base.Enums;
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
  private static final String SCHEMA_DEDUP_FIELD_ANNOTATOR = "primarykey";

  private static enum DedupKeyOption {

    // Use all fields in the topic schema
    ALL,

    // Use fields in the topic schema whose docs match "(?i).*primarykey".
    // If there's no such field, option ALL will be used.
    KEY,

    // Provide a custom dedup schema through property "avro.key.schema.loc"
    CUSTOM
  };

  private static final DedupKeyOption DEFAULT_DEDUP_KEY_OPTION = DedupKeyOption.KEY;

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
   * Obtain the schema used for compaction. If compaction.dedup.key=all, it returns topicSchema.
   * If compaction.dedup.key=key, it returns a schema composed of all fields in topicSchema
   * whose doc matches "(?i).*primarykey". If there's no such field, option "all" will be used.
   * If compaction.dedup.key=custom, it reads the schema from compaction.avro.key.schema.loc.
   * If the read fails, or if the custom key schema is incompatible with topicSchema, option "key" will be used.
   */
  Schema getKeySchema(Job job, Schema topicSchema) throws IOException {
    Schema keySchema = null;
    DedupKeyOption dedupKeyOption = getDedupKeyOption();
    if (dedupKeyOption == DedupKeyOption.ALL) {
      LOG.info("Using all attributes in the schema (except Map, Arrar and Enum fields) for compaction");
      keySchema = AvroUtils.removeUncomparableFields(topicSchema).get();
    } else if (dedupKeyOption == DedupKeyOption.KEY) {
      LOG.info("Using key attributes in the schema for compaction");
      keySchema = AvroUtils.removeUncomparableFields(getKeySchema(topicSchema)).get();
    } else if (keySchemaFileSpecified()) {
      Path keySchemaFile = getKeySchemaFile();
      LOG.info("Using attributes specified in schema file " + keySchemaFile + " for compaction");
      try {
        keySchema = AvroUtils.parseSchemaFromFile(keySchemaFile, this.fs);
      } catch (IOException e) {
        LOG.error("Failed to parse avro schema from " + keySchemaFile
            + ", using key attributes in the schema for compaction");
        keySchema = AvroUtils.removeUncomparableFields(getKeySchema(topicSchema)).get();
      }
      if (!isKeySchemaValid(keySchema, topicSchema)) {
        LOG.warn(String.format("Key schema %s is not compatible with record schema %s.", keySchema, topicSchema)
            + "Using key attributes in the schema for compaction");
        keySchema = AvroUtils.removeUncomparableFields(getKeySchema(topicSchema)).get();
      }
    } else {
      LOG.info("Property " + ConfigurationKeys.COMPACTION_AVRO_KEY_SCHEMA_LOC
          + " not provided. Using key attributes in the schema for compaction");
      keySchema = AvroUtils.removeUncomparableFields(getKeySchema(topicSchema)).get();
    }

    return keySchema;
  }

  /**
   * Returns a schema composed of all fields in topicSchema whose doc match "(?i).*primarykey".
   * If there's no such field, topicSchema itself will be returned.
   */
  private Schema getKeySchema(Schema topicSchema) {
    Preconditions.checkArgument(topicSchema.getType() == Schema.Type.RECORD);

    Optional<Schema> newSchema = getKeySchemaFromRecord(topicSchema);
    if (newSchema.isPresent()) {
      return newSchema.get();
    } else {
      LOG.warn(String.format("No field in the schema of %s is annotated as primarykey. Using all fields for deduping",
          topicSchema.getName()));
      return topicSchema;
    }
  }

  private Optional<Schema> getKeySchema(Field field) {
    switch (field.schema().getType()) {
      case RECORD:
        return getKeySchemaFromRecord(field.schema());
      default:
        if (field.doc().toLowerCase().endsWith(SCHEMA_DEDUP_FIELD_ANNOTATOR)) {
          return Optional.of(field.schema());
        } else {
          return Optional.absent();
        }
    }
  }

  private Optional<Schema> getKeySchemaFromRecord(Schema record) {
    Preconditions.checkArgument(record.getType() == Schema.Type.RECORD);

    List<Field> fields = Lists.newArrayList();
    for (Field field : record.getFields()) {
      Optional<Schema> newFieldSchema = getKeySchema(field);
      if (newFieldSchema.isPresent()) {
        fields.add(new Field(field.name(), newFieldSchema.get(), field.doc(), field.defaultValue()));
      }
    }
    if (!fields.isEmpty()) {
      Schema newSchema = Schema.createRecord(record.getName(), record.getDoc(), record.getName(), false);
      newSchema.setFields(fields);
      return Optional.of(newSchema);
    } else {
      return Optional.absent();
    }
  }

  /**
   * keySchema is valid if a record with newestSchema can be converted to a record with keySchema.
   */
  private boolean isKeySchemaValid(Schema keySchema, Schema topicSchema) {
    return SchemaCompatibility.checkReaderWriterCompatibility(keySchema, topicSchema).getType()
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

  private DedupKeyOption getDedupKeyOption() {
    if (!this.jobProps.contains(ConfigurationKeys.COMPACTION_DEDUP_KEY)) {
      return DEFAULT_DEDUP_KEY_OPTION;
    }
    Optional<DedupKeyOption> option = Enums.getIfPresent(DedupKeyOption.class,
        this.jobProps.getProp(ConfigurationKeys.COMPACTION_DEDUP_KEY).toUpperCase());
    return option.isPresent() ? option.get() : DEFAULT_DEDUP_KEY_OPTION;
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

  @Override
  protected Collection<String> getApplicableFileExtensions() {
    return Lists.newArrayList(AVRO);
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
