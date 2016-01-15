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

package gobblin.compaction.mapreduce.avro;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import gobblin.util.AvroUtils;
import gobblin.util.FileListUtils;


/**
 * A subclass of {@link org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat} for Avro inputfiles.
 * This class is able to handle the case where the input path has subdirs which contain data files, which
 * is not the case with {@link org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat}.
 *
 * @author ziliu
 */
public class AvroKeyRecursiveCombineFileInputFormat
    extends CombineFileInputFormat<AvroKey<GenericRecord>, NullWritable> {

  private static final String COMPACTION_JOB_PREFIX = "compaction.job.";

  /**
   * Properties related to the input format of the compaction job of a dataset.
   */
  private static final String COMPACTION_JOB_MAPRED_MAX_SPLIT_SIZE = COMPACTION_JOB_PREFIX + "mapred.max.split.size";
  private static final long DEFAULT_COMPACTION_JOB_MAPRED_MAX_SPLIT_SIZE = 268435456;
  private static final String COMPACTION_JOB_MAPRED_MIN_SPLIT_SIZE = COMPACTION_JOB_PREFIX + "mapred.min.split.size";
  private static final long DEFAULT_COMPACTION_JOB_MAPRED_MIN_SPLIT_SIZE = 268435456;

  private static final long GET_SPLIT_NUM_FILES_TRHESHOLD = 5000;
  private static final int SPLIT_MAX_NUM_LOCATIONS = 10;
  private static final String AVRO = "avro";

  @Override
  public List<InputSplit> getSplits(JobContext cx) throws IOException {
    setSplitSize(cx);
    List<InputSplit> splits = getSplits(cx, Arrays.asList(getInputPaths(cx)));
    return cleanSplits(splits);
  }

  private void setSplitSize(JobContext cx) {
    super.setMaxSplitSize(cx.getConfiguration().getLong(COMPACTION_JOB_MAPRED_MAX_SPLIT_SIZE,
        DEFAULT_COMPACTION_JOB_MAPRED_MAX_SPLIT_SIZE));
    super.setMinSplitSizeNode(cx.getConfiguration().getLong(COMPACTION_JOB_MAPRED_MIN_SPLIT_SIZE,
        DEFAULT_COMPACTION_JOB_MAPRED_MIN_SPLIT_SIZE));
  }

  private List<InputSplit> getSplits(JobContext cx, List<Path> dirs) throws FileNotFoundException, IOException {

    List<InputSplit> splits = Lists.newArrayList();

    List<Path> subdirs = Lists.newArrayList();
    long totalFileCount = 0;

    FileSystem fs = FileSystem.get(cx.getConfiguration());
    for (Path input : dirs) {
      long count = fs.getContentSummary(input).getFileCount();
      subdirs.add(input);
      if (totalFileCount + count < GET_SPLIT_NUM_FILES_TRHESHOLD) {
        totalFileCount += count;
      } else {
        addAvroFilesInSubdirsToSplits(splits, subdirs, fs, cx);
        subdirs.clear();
        totalFileCount = 0;
      }
    }

    if (totalFileCount > 0) {
      addAvroFilesInSubdirsToSplits(splits, subdirs, fs, cx);
    }
    return splits;
  }

  private void addAvroFilesInSubdirsToSplits(List<InputSplit> splits, List<Path> subdirs, FileSystem fs, JobContext cx)
      throws FileNotFoundException, IOException {
    Map<Schema, List<Path>> filesBySchema = new HashMap<Schema, List<Path>>();
    for (Path file : findAvroFilesInDirs(subdirs, fs)) {
      final Schema schema = AvroUtils.getSchemaFromDataFile(file, fs);
      if (! filesBySchema.containsKey(schema)) {
        filesBySchema.put(schema, new ArrayList<Path>());
      }
      filesBySchema.get(schema).add(file);
    }

    for (Map.Entry<Schema, List<Path>> entry : filesBySchema.entrySet()) {
      List<Path> files = entry.getValue();
      Job helperJob = Job.getInstance(cx.getConfiguration());
      setInputPaths(helperJob, files.toArray(new Path[files.size()]));
      for (InputSplit inputSplit : super.getSplits(helperJob)) {
        splits.add(new AvroCombineFileSplit((CombineFileSplit) inputSplit, entry.getKey()));
      }
    }
  }

  private List<Path> findAvroFilesInDirs(List<Path> dirs, FileSystem fs) throws FileNotFoundException, IOException {
    List<Path> files = Lists.newArrayList();

    for (Path dir : dirs) {
      for (FileStatus status : FileListUtils.listFilesRecursively(fs, dir)) {
        if (FilenameUtils.isExtension(status.getPath().getName(), AVRO)) {
          files.add(status.getPath());
        }
      }
    }
    return files;
  }

  /**
   * Set the number of locations in the split to SPLIT_MAX_NUM_LOCATIONS if it is larger than
   * SPLIT_MAX_NUM_LOCATIONS (MAPREDUCE-5186).
   */
  private List<InputSplit> cleanSplits(List<InputSplit> splits) throws IOException {
    List<InputSplit> cleanedSplits = Lists.newArrayList();

    for (int i = 0; i < splits.size(); i++) {
      AvroCombineFileSplit oldSplit = (AvroCombineFileSplit) splits.get(i);
      String[] locations = oldSplit.getLocations();
      Schema schema = oldSplit.getSchema();

      Preconditions.checkNotNull(locations, "CombineFileSplit.getLocations() returned null");

      if (locations.length > SPLIT_MAX_NUM_LOCATIONS) {
        locations = Arrays.copyOf(locations, SPLIT_MAX_NUM_LOCATIONS);
      }

      cleanedSplits
          .add(new AvroCombineFileSplit(
              oldSplit.getPaths(), oldSplit.getStartOffsets(), oldSplit.getLengths(), locations, schema));
    }
    return cleanedSplits;
  }

  @Override
  public RecordReader<AvroKey<GenericRecord>, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext cx)
      throws IOException {
    return new CombineFileRecordReader<AvroKey<GenericRecord>, NullWritable>((CombineFileSplit) split, cx,
        AvroKeyCombineFileRecordReader.class);
  }
}
