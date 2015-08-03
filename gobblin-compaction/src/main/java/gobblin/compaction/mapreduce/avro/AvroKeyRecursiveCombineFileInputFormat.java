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

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.HadoopUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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


/**
 * A subclass of {@link org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat} for Avro inputfiles.
 * This class is able to handle the case where the input path has subdirs which contain data files, which
 * is not the case with {@link org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat}.
 *
 * @author ziliu
 */
public class AvroKeyRecursiveCombineFileInputFormat
    extends CombineFileInputFormat<AvroKey<GenericRecord>, NullWritable> {
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
    super.setMaxSplitSize(cx.getConfiguration().getLong(ConfigurationKeys.COMPACTION_MAPRED_MAX_SPLIT_SIZE,
        ConfigurationKeys.DEFAULT_COMPACTION_MAPRED_MAX_SPLIT_SIZE));
    super.setMinSplitSizeNode(cx.getConfiguration().getLong(ConfigurationKeys.COMPACTION_MAPRED_MIN_SPLIT_SIZE,
        ConfigurationKeys.DEFAULT_COMPACTION_MAPRED_MIN_SPLIT_SIZE));
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
    List<Path> files = findAvroFilesInDirs(subdirs, fs);
    Job helperJob = Job.getInstance(cx.getConfiguration());
    setInputPaths(helperJob, files.toArray(new Path[files.size()]));
    splits.addAll(super.getSplits(helperJob));
  }

  private List<Path> findAvroFilesInDirs(List<Path> dirs, FileSystem fs) throws FileNotFoundException, IOException {
    List<Path> files = Lists.newArrayList();

    for (Path dir : dirs) {
      for (FileStatus status : HadoopUtils.listStatusRecursive(fs, dir)) {
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
      CombineFileSplit oldSplit = (CombineFileSplit) splits.get(i);
      String[] locations = oldSplit.getLocations();

      Preconditions.checkNotNull(locations, "CombineFileSplit.getLocations() returned null");

      if (locations.length > SPLIT_MAX_NUM_LOCATIONS) {
        locations = Arrays.copyOf(locations, SPLIT_MAX_NUM_LOCATIONS);
      }

      cleanedSplits
          .add(new CombineFileSplit(oldSplit.getPaths(), oldSplit.getStartOffsets(), oldSplit.getLengths(), locations));
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
