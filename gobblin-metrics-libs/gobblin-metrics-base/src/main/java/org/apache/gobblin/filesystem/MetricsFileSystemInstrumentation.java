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

package org.apache.gobblin.filesystem;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;

import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.metrics.ContextAwareTimer;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.util.filesystem.FileSystemInstrumentation;
import org.apache.gobblin.util.filesystem.FileSystemInstrumentationFactory;
import org.apache.gobblin.util.filesystem.FileSystemKey;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link org.apache.hadoop.fs.FileSystem} that extends HDFS and allows instrumentation of certain calls (for example,
 * counting the number of calls to a certain method or measuring latency). For now it is just a skeleton.
 *
 * Using the scheme "instrumented-hdfs" will automatically use this {@link org.apache.hadoop.fs.FileSystem} and work
 * transparently as any other HDFS file system.
 *
 * When modifying this class, tests must be run manually (see InstrumentedHDFSFileSystemTest).
 */
@Slf4j
public class MetricsFileSystemInstrumentation extends FileSystemInstrumentation {

  public static class Factory<S extends ScopeType<S>> extends FileSystemInstrumentationFactory<S> {
    @Override
    public FileSystem instrumentFileSystem(FileSystem fs, SharedResourcesBroker<S> broker,
        ConfigView<S, FileSystemKey> config) {
      return new MetricsFileSystemInstrumentation(fs);
    }
  }

  private MetricContext metricContext;

  protected final Closer closer;

  // Below are HDFS metrics
  @VisibleForTesting
  protected final ContextAwareTimer listStatusTimer;
  @VisibleForTesting
  protected final ContextAwareTimer listFilesTimer;
  @VisibleForTesting
  protected final ContextAwareTimer globStatusTimer;
  @VisibleForTesting
  protected final ContextAwareTimer mkdirTimer;
  @VisibleForTesting
  protected final ContextAwareTimer deleteTimer;
  @VisibleForTesting
  protected final ContextAwareTimer renameTimer;
  @VisibleForTesting
  protected final ContextAwareTimer createTimer;
  @VisibleForTesting
  protected final ContextAwareTimer openTimer;
  @VisibleForTesting
  protected final ContextAwareTimer setOwnerTimer;
  @VisibleForTesting
  protected final ContextAwareTimer getFileStatusTimer;
  @VisibleForTesting
  protected final ContextAwareTimer setPermissionTimer;
  @VisibleForTesting
  protected final ContextAwareTimer setTimesTimer;
  @VisibleForTesting
  protected final ContextAwareTimer appendTimer;
  @VisibleForTesting
  protected final ContextAwareTimer concatTimer;

  private final List<ContextAwareTimer> allTimers;

  private static class TimerContextWithLog implements Closeable {
    Timer.Context context;
    String operation;
    List<Object> parameters;
    long startTick;
    Object result;
    private static final Logger LOG = LoggerFactory.getLogger(TimerContextWithLog.class);
    public TimerContextWithLog (Timer.Context context, String operation, Object... values) {
      this.context = context;
      this.startTick = System.nanoTime();
      this.operation = operation;
      this.parameters = new ArrayList<>(Arrays.asList(values));
      this.result = null;
    }

    public void setResult(Object rst) {
      this.result = rst;
    }

    public void close() {
      long duration = System.nanoTime() - startTick;
      if (result instanceof FileStatus[]) {
        LOG.debug ("HDFS operation {} with {} takes {} nanoseconds and returns {} files", operation, parameters, duration, ((FileStatus[])result).length);
      } else {
        LOG.debug ("HDFS operation {} with {} takes {} nanoseconds", operation, parameters, duration);
      }
      this.context.close();
    }
  }

  public MetricsFileSystemInstrumentation(FileSystem underlying) {
    super(underlying);
    this.closer = Closer.create();
    this.metricContext = new MetricContext.Builder(underlying.getUri() + "_metrics").build();
    this.metricContext = this.closer.register(metricContext);

    this.listStatusTimer = this.metricContext.timer("listStatus");
    this.listFilesTimer = this.metricContext.timer("listFiles");
    this.globStatusTimer = this.metricContext.timer("globStatus");
    this.mkdirTimer = this.metricContext.timer("mkdirs");
    this.renameTimer = this.metricContext.timer("rename");
    this.deleteTimer = this.metricContext.timer("delete");
    this.createTimer = this.metricContext.timer("create");
    this.openTimer = this.metricContext.timer("open");
    this.setOwnerTimer = this.metricContext.timer("setOwner");
    this.getFileStatusTimer = this.metricContext.timer("getFileStatus");
    this.setPermissionTimer = this.metricContext.timer("setPermission");
    this.setTimesTimer = this.metricContext.timer("setTimes");
    this.appendTimer = this.metricContext.timer ("append");
    this.concatTimer = this.metricContext.timer ("concat");

    this.allTimers = ImmutableList.<ContextAwareTimer>builder().add(
        this.listStatusTimer, this.listFilesTimer, this.globStatusTimer, this.mkdirTimer, this.renameTimer,
        this.deleteTimer, this.createTimer, this.openTimer, this.setOwnerTimer, this.getFileStatusTimer, this.setPermissionTimer,
        this.setTimesTimer, this.appendTimer, this.concatTimer
    ).build();
  }

  @Override
  protected void onClose() {
    StringBuilder message = new StringBuilder();
    message.append("========================").append("\n");
    message.append("Statistics for FileSystem: ").append(getUri()).append("\n");
    message.append("------------------------").append("\n");
    message.append("method\tcalls\tmean time(ns)\t99 percentile(ns)").append("\n");
    for (ContextAwareTimer timer : this.allTimers) {
      if (timer.getCount() > 0) {
        message.append(timer.getName()).append("\t").append(timer.getCount()).append("\t").
            append(timer.getSnapshot().getMean()).append("\t").append(timer.getSnapshot().get99thPercentile()).append("\n");
      }
    }
    message.append("------------------------").append("\n");

    log.info(message.toString());
    super.onClose();
  }

  @Override
  public void close()
      throws IOException {
    super.close();
    // Should print out statistics here
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#mkdirs(Path, FsPermission)}
   */
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    try (Closeable context = new TimerContextWithLog(mkdirTimer.time(), "mkdirs", f, permission)) {
      return super.mkdirs (f, permission);
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#rename(Path, Path)}
   */
  public boolean rename (Path src, Path dst) throws IOException {
    try (Closeable context =  new TimerContextWithLog(renameTimer.time(), "rename", src, dst)) {
      return super.rename(src, dst);
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#delete(Path, boolean)}
   */
  public boolean delete (Path f, boolean recursive) throws  IOException {
    try (Closeable context = new TimerContextWithLog(deleteTimer.time(), "delete", f, recursive)) {
      return super.delete (f, recursive);
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#listStatus(Path)}
   */
  public FileStatus[] listStatus(Path path) throws IOException {
   try (TimerContextWithLog context = new TimerContextWithLog(listStatusTimer.time(), "listStatus", path)) {
      FileStatus[] statuses = super.listStatus(path);
      context.setResult(statuses);
      return statuses;
    }
  }

  /**
   * Add timer metrics to  {@link DistributedFileSystem#globStatus(Path)}
   */
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    try (TimerContextWithLog context = new TimerContextWithLog(globStatusTimer.time(), "globStatus", pathPattern)) {
      FileStatus[] statuses = super.globStatus(pathPattern);
      context.setResult(statuses);
      return statuses;
    }
  }

  /**
   * Add timer metrics to  {@link DistributedFileSystem#globStatus(Path, PathFilter)}
   */
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    try (TimerContextWithLog context = new TimerContextWithLog(globStatusTimer.time(), "globStatus", pathPattern, filter)) {
      FileStatus[] statuses = super.globStatus(pathPattern, filter);
      context.setResult(statuses);
      return statuses;
    }
  }

  /**
   * Add timer metrics to {@link FileSystem#listFiles(Path, boolean)}
   */
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException {
    try (Closeable context = new TimerContextWithLog(this.listFilesTimer.time(), "listFiles", f, recursive)) {
      return super.listFiles(f, recursive);
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#create(Path, FsPermission, boolean, int, short, long, Progressable)}
   */
  public FSDataOutputStream create(Path f,
    FsPermission permission,
    boolean overwrite,
    int bufferSize,
    short replication,
    long blockSize,
    Progressable progress) throws IOException {
    try (Closeable context = new TimerContextWithLog(this.createTimer.time(), "create", f, permission, overwrite, bufferSize, replication, blockSize, progress)) {
      return super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#open(Path, int)}
   */
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    try (Closeable context = new TimerContextWithLog(this.openTimer.time(), "open", f, bufferSize)) {
      return super.open(f, bufferSize);
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#setOwner(Path, String, String)}
   */
  public void setOwner(Path f, String user, String group) throws IOException {
    try (Closeable context = new TimerContextWithLog(this.setOwnerTimer.time(), "setOwner", f, user, group)) {
      super.setOwner(f, user, group);
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#getFileStatus(Path)}
   */
  public FileStatus getFileStatus (Path f) throws IOException {
    try (Closeable context = new TimerContextWithLog(this.getFileStatusTimer.time(), "getFileStatus", f)) {
      return super.getFileStatus(f);
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#setPermission(Path, FsPermission)}
   */
  public void setPermission (Path f, final FsPermission permission) throws IOException {
    try (Closeable context = new TimerContextWithLog(this.setPermissionTimer.time(), "setPermission", f, permission)) {
      super.setPermission(f, permission);
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#setTimes(Path, long, long)}
   */
  public void setTimes (Path f, long t, long a) throws IOException {
    try (Closeable context = new TimerContextWithLog(this.setTimesTimer.time(), "setTimes", f, t, a)) {
      super.setTimes(f, t, a);
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#append(Path, int, Progressable)}
   */
  public FSDataOutputStream append (Path p, final int bufferSize, Progressable progress) throws IOException {
    try (Closeable context = new TimerContextWithLog(this.appendTimer.time(), "append", p)) {
      return super.append(p, bufferSize, progress);
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#concat(Path, Path[])}
   */
  public void concat (Path trg, Path [] psrcs) throws IOException  {
    try (Closeable context = new TimerContextWithLog(this.concatTimer.time(), "concat", trg, psrcs)) {
      super.concat(trg, psrcs);
    }
  }
}
