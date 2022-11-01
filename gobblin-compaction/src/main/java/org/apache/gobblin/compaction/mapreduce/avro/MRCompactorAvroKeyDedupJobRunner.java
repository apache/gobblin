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

package org.apache.gobblin.compaction.mapreduce.avro;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import org.apache.commons.io.FilenameUtils;
import org.apache.gobblin.compaction.dataset.Dataset;
import org.apache.gobblin.compaction.mapreduce.MRCompactorJobRunner;
import org.apache.gobblin.util.AvroUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A subclass of {@link org.apache.gobblin.compaction.mapreduce.MRCompactorJobRunner} that configures
 * and runs MR compaction job for Avro data.
 *
 * To dedup using entire records set compaction.use.all.attributes=true. Otherwise, a schema needs
 * to be provided by compaction.avro.key.schema.loc, based on which the dedup is performed.
 *
 * @author Ziyang Liu
 */
public class MRCompactorAvroKeyDedupJobRunner extends MRCompactorJobRunner {
  private static final Logger LOG = LoggerFactory.getLogger(MRCompactorAvroKeyDedupJobRunner.class);

  private static final String COMPACTION_JOB_PREFIX = "compaction.job.";

  /**
   * If true, the latest schema, determined from the input files, will be used as single schema for all input files,
   * otherwise, the avro each input file will be determined and splits will be created with respect to the input file's schema
   */
  public static final String COMPACTION_JOB_AVRO_SINGLE_INPUT_SCHEMA =
      COMPACTION_JOB_PREFIX + "avro.single.input.schema";

  /**
   * Properties related to the avro dedup compaction job of a dataset.
   */
  public static final String COMPACTION_JOB_AVRO_KEY_SCHEMA_LOC = COMPACTION_JOB_PREFIX + "avro.key.schema.loc";
  public static final String COMPACTION_JOB_DEDUP_KEY = COMPACTION_JOB_PREFIX + "dedup.key";
  public static final String COMPACTION_JOB_KEY_FIELD_BLACKLIST = COMPACTION_JOB_PREFIX + "key.fieldBlacklist";

  private static final String AVRO = "avro";
  private static final String SCHEMA_DEDUP_FIELD_ANNOTATOR = "primarykey";

  public enum DedupKeyOption {

    // Use all fields in the topic schema
    ALL,

    // Use fields in the topic schema whose docs match "(?i).*primarykey".
    // If there's no such field, option ALL will be used.
    KEY,

    // Provide a custom dedup schema through property "avro.key.schema.loc"
    CUSTOM
  }

  public static final DedupKeyOption DEFAULT_DEDUP_KEY_OPTION = DedupKeyOption.KEY;

  private final boolean useSingleInputSchema;

  public MRCompactorAvroKeyDedupJobRunner(Dataset dataset, FileSystem fs) {
    super(dataset, fs);
    this.useSingleInputSchema = this.dataset.jobProps().getPropAsBoolean(COMPACTION_JOB_AVRO_SINGLE_INPUT_SCHEMA, true);
  }

  @Override
  protected void configureJob(Job job) throws IOException {
    super.configureJob(job);
    configureSchema(job);
  }

  private void configureSchema(Job job) throws IOException {
    Schema newestSchema = getNewestSchemaFromSource(job, this.fs);
    if (this.useSingleInputSchema) {
      AvroJob.setInputKeySchema(job, newestSchema);
    }
    AvroJob.setMapOutputKeySchema(job, this.shouldDeduplicate ? getKeySchema(job, newestSchema) : newestSchema);
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
  @VisibleForTesting
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
      LOG.info("Property " + COMPACTION_JOB_AVRO_KEY_SCHEMA_LOC
          + " not provided. Using key attributes in the schema for compaction");
      keySchema = AvroUtils.removeUncomparableFields(getKeySchema(topicSchema)).get();
    }

    return keySchema;
  }

  /**
   * Returns a schema composed of all fields in topicSchema whose doc match "(?i).*primarykey".
   * If there's no such field, topicSchema itself will be returned.
   */
  public static Schema getKeySchema(Schema topicSchema) {
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

  public static Optional<Schema> getKeySchema(Field field) {
    switch (field.schema().getType()) {
      case RECORD:
        return getKeySchemaFromRecord(field.schema());
      default:
        if (field.doc() != null && field.doc().toLowerCase().endsWith(SCHEMA_DEDUP_FIELD_ANNOTATOR)) {
          return Optional.of(field.schema());
        } else {
          return Optional.absent();
        }
    }
  }

  public static Optional<Schema> getKeySchemaFromRecord(Schema record) {
    Preconditions.checkArgument(record.getType() == Schema.Type.RECORD);

    List<Field> fields = Lists.newArrayList();
    for (Field field : record.getFields()) {
      Optional<Schema> newFieldSchema = getKeySchema(field);
      if (newFieldSchema.isPresent()) {
        fields.add(AvroCompatibilityHelper.createSchemaField(field.name(), newFieldSchema.get(), field.doc(),
            AvroUtils.getCompatibleDefaultValue(field)));
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
  public static boolean isKeySchemaValid(Schema keySchema, Schema topicSchema) {
    return SchemaCompatibility.checkReaderWriterCompatibility(keySchema, topicSchema).getType()
        .equals(SchemaCompatibilityType.COMPATIBLE);
  }

  public static Schema getNewestSchemaFromSource(Job job, FileSystem fs) throws IOException {
    Path[] sourceDirs = FileInputFormat.getInputPaths(job);

    List<FileStatus> files = new ArrayList<FileStatus>();

    for (Path sourceDir : sourceDirs) {
      files.addAll(Arrays.asList(fs.listStatus(sourceDir)));
    }

    Collections.sort(files, new LastModifiedDescComparator());

    for (FileStatus file : files) {
      Schema schema = getNewestSchemaFromSource(file.getPath(), fs);
      if (schema != null) {
        return schema;
      }
    }
    return null;
  }

  public static Schema getNewestSchemaFromSource(Path sourceDir, FileSystem fs) throws IOException {
    FileStatus[] files = fs.listStatus(sourceDir);
    Arrays.sort(files, new LastModifiedDescComparator());
    for (FileStatus status : files) {
      if (status.isDirectory()) {
        Schema schema = getNewestSchemaFromSource(status.getPath(), fs);
        if (schema != null)
          return schema;
      } else if (FilenameUtils.isExtension(status.getPath().getName(), AVRO)) {
        return AvroUtils.getSchemaFromDataFile(status.getPath(), fs);
      }
    }
    return null;
  }

  private DedupKeyOption getDedupKeyOption() {
    if (!this.dataset.jobProps().contains(COMPACTION_JOB_DEDUP_KEY)) {
      return DEFAULT_DEDUP_KEY_OPTION;
    }
    Optional<DedupKeyOption> option = Enums.getIfPresent(DedupKeyOption.class,
        this.dataset.jobProps().getProp(COMPACTION_JOB_DEDUP_KEY).toUpperCase());
    return option.isPresent() ? option.get() : DEFAULT_DEDUP_KEY_OPTION;
  }

  private boolean keySchemaFileSpecified() {
    return this.dataset.jobProps().contains(COMPACTION_JOB_AVRO_KEY_SCHEMA_LOC);
  }

  private Path getKeySchemaFile() {
    return new Path(this.dataset.jobProps().getProp(COMPACTION_JOB_AVRO_KEY_SCHEMA_LOC));
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
  public static class LastModifiedDescComparator implements Comparator<FileStatus>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public int compare(FileStatus fs1, FileStatus fs2) {
      if (fs2.getModificationTime() < fs1.getModificationTime()) {
        return -1;
      } else if (fs2.getModificationTime() > fs1.getModificationTime()) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
