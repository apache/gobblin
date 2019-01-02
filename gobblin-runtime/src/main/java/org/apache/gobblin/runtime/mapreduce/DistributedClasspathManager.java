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

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import org.apache.gobblin.broker.ResourceInstance;
import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.ScopedConfigView;
import org.apache.gobblin.broker.iface.SharedResourceFactory;
import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.util.filesystem.FileSystemFactory;
import org.apache.gobblin.util.filesystem.FileSystemKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ClassUtil;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import lombok.extern.slf4j.Slf4j;


/**
 * A class to add artifacts to the classpath of an MR job making sure various jobs share the artifacts instead of
 * re-uploading to HDFS every time.
 */
@Slf4j
public class DistributedClasspathManager {

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();
  private static final int WAITING_TIME_ON_IMCOMPLETE_UPLOAD = 3000;

  private final FileSystem fs;
  private final Path rootPath;

  /**
   * {@link org.apache.gobblin.broker.iface.SharedResourceKey} for creating {@link DistributedClasspathManager} using
   * {@link SharedResourcesBroker}.
   */
  public static class DistributedClasspathManagerKey extends FileSystemKey {

    private final Path rootPath;

    public DistributedClasspathManagerKey(URI uri, Configuration configuration) {
      this(uri, configuration,
          new Path(String.format("/tmp/%s/%s", DistributedClasspathManager.class.getSimpleName(),
              UUID.randomUUID().toString())));
    }

    public DistributedClasspathManagerKey(URI uri, Configuration configuration, Path rootPath) {
      super(uri, configuration);
      this.rootPath = rootPath;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      DistributedClasspathManagerKey that = (DistributedClasspathManagerKey) o;

      return rootPath != null ? rootPath.equals(that.rootPath) : that.rootPath == null;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (rootPath != null ? rootPath.hashCode() : 0);
      return result;
    }
  }

  /**
   * {@link SharedResourceFactory} for creating {@link DistributedClasspathManager} using
   * {@link SharedResourcesBroker}.
   */
  public static class DistributedClasspathManagerFactory<S extends ScopeType<S>>
      implements SharedResourceFactory<DistributedClasspathManager, DistributedClasspathManagerKey, S> {

    @Override
    public String getName() {
      return DistributedClasspathManager.class.getSimpleName();
    }

    @Override
    public SharedResourceFactoryResponse<DistributedClasspathManager> createResource(
        SharedResourcesBroker<S> sharedResourcesBroker,
        ScopedConfigView<S, DistributedClasspathManagerKey> scopedConfigView) throws NotConfiguredException {
      try {
        return new ResourceInstance<>(new DistributedClasspathManager(
            sharedResourcesBroker.getSharedResource(new FileSystemFactory<S>(), scopedConfigView.getKey()),
            scopedConfigView.getKey().rootPath));
      } catch (IOException exc) {
        throw new RuntimeException(exc);
      }
    }

    @Override
    public S getAutoScope(SharedResourcesBroker<S> sharedResourcesBroker,
        ConfigView<S, DistributedClasspathManagerKey> configView) {
      return sharedResourcesBroker.selfScope().getType().rootScope();
    }
  }

  private final LocalFileSystem lfs;

  public DistributedClasspathManager(FileSystem fs, Path rootPath) throws IOException {
    this.fs = fs;
    this.rootPath = rootPath;
    this.lfs = FileSystem.getLocal(new Configuration());

    if (!this.fs.exists(this.rootPath)) {
      this.fs.mkdirs(this.rootPath);
      this.fs.setPermission(this.rootPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }
  }

  /**
   * Add the artifact containing the specified class to the classpath of the input job.
   */
  public void addToDistributedClasspathByClass(Job job, Class<?> klazz, Path privateJarsDir, int maxAttempts)
      throws IOException {

    String jarStr = ClassUtil.findContainingJar(klazz);
    if (Strings.isNullOrEmpty(jarStr)) {
      // Usually happens when running on IDE
      return;
    }
    if (fs.getScheme().equals("file")) {
      // not running in distributed mode
      return;
    }
    addFileToClasspath(job, lfs.getFileStatus(new Path(jarStr)), privateJarsDir, maxAttempts);
  }

  /**
   * Add the artifacts contained in the {@code jarFileList} to the classpath of the input job.
   */
  public void addToDistributedClasspath(Job job, String jarFileList, Path privateJarsDir, int maxAttempts)
      throws IOException {

    if (Strings.isNullOrEmpty(jarFileList)) {
      return;
    }
    if (fs.getScheme().equals("file")) {
      // not running in distributed mode
      return;
    }

    // jarFileList is a ',' separated string
    for (String jarFile : SPLITTER.split(jarFileList)) {
      Path srcJarFile = new Path(jarFile);
      // A jar file can be specified with glob pattern
      for (FileStatus status : lfs.globStatus(srcJarFile)) {
        addFileToClasspath(job, status, privateJarsDir, maxAttempts);
      }
    }
  }

  private void addFileToClasspath(Job job, FileStatus fileStatus, Path privateJarsDir, int maxAttempts)
      throws IOException {

    Path file = fileStatus.getPath();

    // SNAPSHOT files should not be shared, as different jobs may be using different versions of it
    Path baseDir = file.getName().contains("SNAPSHOT") ? privateJarsDir : this.rootPath;
    // DistributedCache requires absolute path, so we need to use makeQualified.
    Path destFilePath = new Path(this.fs.makeQualified(baseDir), file.getName());

    // For each FileStatus there are chances it could fail in copying at the first attempt, due to file-existence
    // or file-copy is ongoing by other job instance since all Gobblin jobs share the same jar file directory.
    int attempt = 1;
    while (true) {
      try {
        if (this.fs.exists(destFilePath)) {
          // Assuring destJarFile exists and the size of file on targetPath matches the one on local path.
          if (this.fs.getFileStatus(destFilePath).getLen() == fileStatus.getLen()) {
            break;
          }
          Thread.sleep(WAITING_TIME_ON_IMCOMPLETE_UPLOAD);
        } else {
          // Copy the file from local file system to HDFS
          this.fs.copyFromLocalFile(file, destFilePath);
          this.fs.setPermission(destFilePath, new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.NONE));
          log.info("Copied {}", destFilePath);
          break;
        }
      } catch (IOException | InterruptedException e) {
        attempt++;
        if (attempt > maxAttempts) {
          log.error("Fail to copy file {} after {} attempts.", destFilePath, maxAttempts);
          throw new IOException(e);
        }
      }
    }

    // Then add the file on HDFS to the classpath
    job.addFileToClassPath(destFilePath);
  }
}
