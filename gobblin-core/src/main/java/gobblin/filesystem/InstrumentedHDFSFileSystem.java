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

package gobblin.filesystem;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;

import com.google.common.io.Closer;
import gobblin.metrics.MetricContext;
import org.apache.hadoop.conf.Configuration;
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
public class InstrumentedHDFSFileSystem extends DistributedFileSystem {

  public static final String INSTRUMENTED_HDFS_SCHEME = "instrumented-hdfs";
  public static final String HDFS_METRIC_CONTEXT_NAME = "hdfsMetricContext";
  private static final String HDFS_SCHEME = "hdfs";
  private MetricContext metricContext;

  protected final Closer closer;

  // Below are HDFS metrics
  @VisibleForTesting
  protected final Timer listStatusTimer;
  @VisibleForTesting
  protected final Timer listFilesTimer;
  @VisibleForTesting
  protected final Timer globStatusTimer;
  @VisibleForTesting
  protected final Timer mkdirTimer;
  @VisibleForTesting
  protected final Timer deleteTimer;
  @VisibleForTesting
  protected final Timer renameTimer;
  @VisibleForTesting
  protected final Timer createTimer;
  @VisibleForTesting
  protected final Timer openTimer;
  @VisibleForTesting
  protected final Timer setOwnerTimer;
  @VisibleForTesting
  protected final Timer getFileStatusTimer;
  @VisibleForTesting
  protected final Timer setPermissionTimer;
  @VisibleForTesting
  protected final Timer setTimesTimer;
  @VisibleForTesting
  protected final Timer appendTimer;
  @VisibleForTesting
  protected final Timer concatTimer;

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

  public InstrumentedHDFSFileSystem() {
    this.closer = Closer.create();
    this.metricContext = new MetricContext.Builder(HDFS_METRIC_CONTEXT_NAME).build();
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
  }

  @Override
  public String getScheme() {
    return INSTRUMENTED_HDFS_SCHEME;
  }

  @Override
  public void initialize(URI uri, Configuration conf)
      throws IOException {
    super.initialize(InstrumentedFileSystemUtils.replaceScheme(uri, INSTRUMENTED_HDFS_SCHEME, HDFS_SCHEME), conf);
  }

  @Override
  protected URI getCanonicalUri() {
    return InstrumentedFileSystemUtils.replaceScheme(super.getCanonicalUri(), HDFS_SCHEME, INSTRUMENTED_HDFS_SCHEME);
  }

  @Override
  public URI getUri() {
    return InstrumentedFileSystemUtils.replaceScheme(super.getUri(), HDFS_SCHEME, INSTRUMENTED_HDFS_SCHEME);
  }

  @Override
  protected URI canonicalizeUri(URI uri) {
    return InstrumentedFileSystemUtils.replaceScheme(super.canonicalizeUri(uri), HDFS_SCHEME, INSTRUMENTED_HDFS_SCHEME);
  }

  @Override
  public void close()
      throws IOException {
    super.close();
    // Should print out statistics here
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#mkdir(Path, FsPermission)}
   */
  public boolean mkdir(Path f, FsPermission permission) throws IOException {
    try (Closeable context = new TimerContextWithLog(mkdirTimer.time(), "mkdir", f, permission)) {
      return super.mkdir (f, permission);
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#mkdirs(Path, FsPermission)}
   */
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    try (Closeable context = new TimerContextWithLog(mkdirTimer.time(), "mkdirs", f, permission)) {
      return super.mkdirs (f, permission);
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#rename(Path, Path)}
   */
  public boolean rename (Path src, Path dst) throws IOException {
    try (Closeable context =  new TimerContextWithLog(renameTimer.time(), "rename", src, dst)) {
      return super.rename(src, dst);
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#delete(Path, boolean)}
   */
  public boolean delete (Path f, boolean recursive) throws  IOException {
    try (Closeable context = new TimerContextWithLog(deleteTimer.time(), "delete", f, recursive)) {
      return super.delete (f, recursive);
    } catch (IOException e) {
      throw e;
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
    } catch (IOException e) {
      throw e;
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
    } catch (IOException e) {
      throw e;
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
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link FileSystem#listFiles(Path, boolean)}
   */
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException {
    try (Closeable context = new TimerContextWithLog(this.listFilesTimer.time(), "listFiles", f, recursive)) {
      return super.listFiles(f, recursive);
    } catch (IOException e) {
      throw e;
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
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#open(Path, int)}
   */
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    try (Closeable context = new TimerContextWithLog(this.openTimer.time(), "open", f, bufferSize)) {
      return super.open(f, bufferSize);
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#setOwner(Path, String, String)}
   */
  public void setOwner(Path f, String user, String group) throws IOException {
    try (Closeable context = new TimerContextWithLog(this.setOwnerTimer.time(), "setOwner", f, user, group)) {
      super.setOwner(f, user, group);
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#getFileStatus(Path)}
   */
  public FileStatus getFileStatus (Path f) throws IOException {
    try (Closeable context = new TimerContextWithLog(this.getFileStatusTimer.time(), "getFileStatus", f)) {
      return super.getFileStatus(f);
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#setPermission(Path, FsPermission)}
   */
  public void setPermission (Path f, final FsPermission permission) throws IOException {
    try (Closeable context = new TimerContextWithLog(this.setPermissionTimer.time(), "setPermission", f, permission)) {
      super.setPermission(f, permission);
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#setTimes(Path, long, long)}
   */
  public void setTimes (Path f, long t, long a) throws IOException {
    try (Closeable context = new TimerContextWithLog(this.setTimesTimer.time(), "setTimes", f, t, a)) {
      super.setTimes(f, t, a);
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#append(Path, int, Progressable)}
   */
  public FSDataOutputStream append (Path p, final int bufferSize, Progressable progress) throws IOException {
    try (Closeable context = new TimerContextWithLog(this.appendTimer.time(), "append", p)) {
      return super.append(p, bufferSize, progress);
    } catch (IOException e) {
      throw e;
    }
  }

  /**
   * Add timer metrics to {@link DistributedFileSystem#concat(Path, Path[])}
   */
  public void concat (Path trg, Path [] psrcs) throws IOException  {
    try (Closeable context = new TimerContextWithLog(this.concatTimer.time(), "concat", trg, psrcs)) {
      super.concat(trg, psrcs);
    } catch (IOException e) {
      throw e;
    }
  }
}
