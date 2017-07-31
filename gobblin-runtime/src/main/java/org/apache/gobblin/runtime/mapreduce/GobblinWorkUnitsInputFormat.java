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

package org.apache.gobblin.runtime.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;


/**
 * An input format for reading Gobblin inputs (work unit and multi work unit files).
 */
@Slf4j
public class GobblinWorkUnitsInputFormat extends InputFormat<LongWritable, Text> {

  private static final String MAX_MAPPERS = GobblinWorkUnitsInputFormat.class.getName() + ".maxMappers";

  /**
   * Set max mappers used in MR job.
   */
  public static void setMaxMappers(Job job, int maxMappers) {
    job.getConfiguration().setInt(MAX_MAPPERS, maxMappers);
  }

  public static int getMaxMapper(Configuration conf) {
    return conf.getInt(MAX_MAPPERS, Integer.MAX_VALUE);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context)
      throws IOException, InterruptedException {

    Path[] inputPaths = FileInputFormat.getInputPaths(context);
    if (inputPaths == null || inputPaths.length == 0) {
      throw new IOException("No input found!");
    }

    List<String> allPaths = Lists.newArrayList();

    for (Path path : inputPaths) {
      // path is a single work unit / multi work unit
      FileSystem fs = path.getFileSystem(context.getConfiguration());
      FileStatus[] inputs = fs.listStatus(path);

      if (inputs == null) {
        throw new IOException(String.format("Path %s does not exist.", path));
      }
      log.info(String.format("Found %d input files at %s: %s", inputs.length, path, Arrays.toString(inputs)));
      for (FileStatus input : inputs) {
        allPaths.add(input.getPath().toString());
      }
    }

    int maxMappers = getMaxMapper(context.getConfiguration());
    int numTasksPerMapper =
        allPaths.size() % maxMappers == 0 ? allPaths.size() / maxMappers : allPaths.size() / maxMappers + 1;

    List<InputSplit> splits = Lists.newArrayList();
    Iterator<String> pathsIt = allPaths.iterator();
    while (pathsIt.hasNext()) {
      Iterator<String> limitedIterator = Iterators.limit(pathsIt, numTasksPerMapper);
      splits.add(new GobblinSplit(Lists.newArrayList(limitedIterator)));
    }

    return splits;
  }

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new GobblinRecordReader((GobblinSplit) split);
  }

  /**
   * {@link InputSplit} that just contain the work unit / multi work unit files that each mapper should process.
   */
  @AllArgsConstructor
  @NoArgsConstructor
  @Builder
  @EqualsAndHashCode
  public static class GobblinSplit extends InputSplit implements Writable {

    /**
     * A list of {@link Path}s containing work unit / multi work unit.
     */
    @Getter
    @Singular
    private List<String> paths;

    @Override
    public void write(DataOutput out)
        throws IOException {
      out.writeInt(this.paths.size());
      for (String path : this.paths) {
        out.writeUTF(path);
      }
    }

    @Override
    public void readFields(DataInput in)
        throws IOException {
      int numPaths = in.readInt();
      this.paths = Lists.newArrayList();
      for (int i = 0; i < numPaths; i++) {
        this.paths.add(in.readUTF());
      }
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
   * Returns records containing the name of the work unit / multi work unit files to process.
   */
  public static class GobblinRecordReader extends RecordReader<LongWritable, Text> {
    private int currentIdx = -1;
    private final List<String> paths;
    private final int totalPaths;

    public GobblinRecordReader(GobblinSplit split) {
      this.paths = split.getPaths();
      this.totalPaths = this.paths.size();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue()
        throws IOException, InterruptedException {
      this.currentIdx++;
      return this.currentIdx < this.totalPaths;
    }

    @Override
    public LongWritable getCurrentKey()
        throws IOException, InterruptedException {
      return new LongWritable(this.currentIdx);
    }

    @Override
    public Text getCurrentValue()
        throws IOException, InterruptedException {
      return new Text(this.paths.get(this.currentIdx));
    }

    @Override
    public float getProgress()
        throws IOException, InterruptedException {
      return (float) this.currentIdx / (float) this.totalPaths;
    }

    @Override
    public void close()
        throws IOException {
    }
  }
}
