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

package gobblin.runtime.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.google.common.collect.Lists;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * An input format for reading Gobblin inputs (work unit and multi work unit files).
 */
@Slf4j
public class GobblinWorkUnitsInputFormat extends InputFormat<LongWritable, Text> {

  @Override
  public List<InputSplit> getSplits(JobContext context)
      throws IOException, InterruptedException {

    Path[] inputPaths = FileInputFormat.getInputPaths(context);
    if (inputPaths == null || inputPaths.length == 0) {
      throw new IOException("No input found!");
    }

    List<InputSplit> splits = Lists.newArrayList();

    for (Path path : inputPaths) {
      // path is a single work unit / multi work unit
      FileSystem fs = path.getFileSystem(context.getConfiguration());
      FileStatus[] inputs = fs.listStatus(path);

      if (inputs == null) {
        throw new IOException(String.format("Path %s does not exist.", path));
      }
      log.info(String.format("Found %d input files at %s: %s", inputs.length, path, Arrays.toString(inputs)));
      for (FileStatus input : inputs) {
        splits.add(new GobblinSplit(input.getPath().toString()));
      }
    }
    return splits;
  }

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new GobblinRecordReader((GobblinSplit) split);
  }

  /**
   * {@link InputSplit} that just contain the work unit / multi work unit file that each mapper should process.
   */
  @AllArgsConstructor
  @NoArgsConstructor
  public static class GobblinSplit extends InputSplit implements Writable {

    /**
     * A directory {@link Path} containing a file for each work unit / multi work unit.
     */
    @Getter
    private String path;

    @Override
    public void write(DataOutput out)
        throws IOException {
      out.writeUTF(this.path);
    }

    @Override
    public void readFields(DataInput in)
        throws IOException {
      this.path = in.readUTF();
    }

    @Override
    public long getLength()
        throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public String[] getLocations()
        throws IOException, InterruptedException {
      return new String[0];
    }
  }

  /**
   * Returns a single record containing the name of the work unit / multi work unit file to process.
   */
  public static class GobblinRecordReader extends RecordReader<LongWritable, Text> {
    private String path;
    private int keyValuesRead = 0;

    public GobblinRecordReader(GobblinSplit split) {
      this.path = split.getPath();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue()
        throws IOException, InterruptedException {
      return keyValuesRead++ < 1;
    }

    @Override
    public LongWritable getCurrentKey()
        throws IOException, InterruptedException {
      return new LongWritable(1);
    }

    @Override
    public Text getCurrentValue()
        throws IOException, InterruptedException {
      return new Text(this.path);
    }

    @Override
    public float getProgress()
        throws IOException, InterruptedException {
      return keyValuesRead * 1.0f;
    }

    @Override
    public void close()
        throws IOException {
    }
  }
}
