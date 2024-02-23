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

package org.apache.gobblin.data.management.version.finder;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Stack;
import java.util.regex.Pattern;
import org.apache.gobblin.data.management.version.FileSystemDatasetVersion;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.util.PathUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;


/**
 * Class to find {@link FileSystemDataset} versions in the file system.
 *
 * Concrete subclasses should implement a ({@link org.apache.hadoop.fs.FileSystem}, {@link java.util.Properties})
 * constructor to be instantiated.
 *
 * Provides a callback {@link AbstractDatasetVersionFinder#getDatasetVersion(Path, FileStatus)} which subclasses need to
 * implement.
 *
 * @param <T> Type of {@link org.apache.gobblin.data.management.version.FileSystemDatasetVersion} expected from this class.
 */
public abstract class AbstractDatasetVersionFinder<T extends FileSystemDatasetVersion> implements VersionFinder<T> {

  protected FileSystem fs;

  public AbstractDatasetVersionFinder(FileSystem fs, Properties props) {
    this.fs = fs;
  }

  public AbstractDatasetVersionFinder(FileSystem fs) {
    this(fs, new Properties());
  }

  /**
   * Find dataset versions in the input {@link org.apache.hadoop.fs.Path}. Dataset versions are subdirectories of the
   * input {@link org.apache.hadoop.fs.Path} representing a single manageable unit in the dataset.
   * See {@link org.apache.gobblin.data.management.retention.DatasetCleaner} for more information.
   *
   * @param dataset {@link org.apache.hadoop.fs.Path} to directory containing all versions of a dataset.
   * @return Map of {@link org.apache.gobblin.data.management.version.DatasetVersion} and {@link org.apache.hadoop.fs.FileStatus}
   *        for each dataset version found.
   * @throws IOException
   */
  @Override
  public Collection<T> findDatasetVersions(Dataset dataset) throws IOException {
    FileSystemDataset fsDataset = (FileSystemDataset) dataset;
    Path versionGlobStatus = new Path(fsDataset.datasetRoot(), globVersionPattern());
    FileStatus[] dataSetVersionPaths = this.fs.globStatus(versionGlobStatus);

    List<T> dataSetVersions = Lists.newArrayList();
    for (FileStatus dataSetVersionPath : dataSetVersionPaths) {
      T datasetVersion =
          getDatasetVersion(PathUtils.relativizePath(dataSetVersionPath.getPath(), fsDataset.datasetRoot()),
              dataSetVersionPath);
      if (datasetVersion != null) {
        dataSetVersions.add(datasetVersion);
      }
    }

    return dataSetVersions;
  }

  /**
   * Find dataset version in the input {@link org.apache.gobblin.dataset}. Dataset versions are subdirectories of the
   * input {@link org.apache.gobblin.dataset} representing a single manageable unit in the dataset.
   *
   * @param dataset {@link org.apache.gobblin.dataset} to directory containing all versions of a dataset
   * @return - Returns an iterator for fetching each dataset version found.
   * @throws IOException
   */
  @Override
  public RemoteIterator<T> findDatasetVersion(Dataset dataset) throws IOException {
    FileSystemDataset fsDataset = (FileSystemDataset) dataset;
    Path versionGlobStatus = new Path(fsDataset.datasetRoot(), globVersionPattern());
    return getDatasetVersionIterator(fsDataset.datasetRoot(), getRegexPattern(versionGlobStatus.toString()));
  }

  /**
   * Returns an iterator to fetch the dataset versions for the datasets whose path {@link org.apache.hadoop.fs.Path}
   * starts with the root and matches the globPattern passed
   *
   * @param root - Path of the root from which the Dataset Versions have to be returned
   * @param pathPattern - Pattern to match the dataset version path
   * @return - an iterator of matched data versions
   * @throws IOException
   */
  public RemoteIterator<T> getDatasetVersionIterator(Path root, String pathPattern) throws IOException {
    Stack<RemoteIterator<FileStatus>> iteratorStack = new Stack<>();
    RemoteIterator<FileStatus> fsIterator = fs.listStatusIterator(root);
    iteratorStack.push(fsIterator);
    return new RemoteIterator<T>() {
      FileStatus nextFileStatus = null;
      boolean isNextFileStatusProcessed = false;

      @Override
      public boolean hasNext() throws IOException {
        if (iteratorStack.isEmpty()) {
          return false;
        }
        // No need to process if the next() has not been called
        if (nextFileStatus != null && !isNextFileStatusProcessed) {
          return true;
        }
        nextFileStatus = fetchNextFileStatus(iteratorStack, pathPattern);
        isNextFileStatusProcessed = false;
        return nextFileStatus != null;
      }

      @Override
      public T next() throws IOException {
        if (nextFileStatus == null || isNextFileStatusProcessed) {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
        }
        T datasetVersion = getDatasetVersion(PathUtils.relativizePath(nextFileStatus.getPath(), root), nextFileStatus);
        isNextFileStatusProcessed = true;
        return datasetVersion;
      }
    };
  }

  /**
   * Helper method to find the next filestatus matching the globPattern.
   * This uses a stack to keep track of the fileStatusIterator returned at each subpaths
   *
   * @param iteratorStack
   * @param globPattern
   * @return
   * @throws IOException
   */
  private FileStatus fetchNextFileStatus(Stack<RemoteIterator<FileStatus>> iteratorStack,
      String globPattern) throws IOException {
    while (!iteratorStack.isEmpty()) {
      RemoteIterator<FileStatus> latestfsIterator = iteratorStack.pop();
      while (latestfsIterator.hasNext()) {
        FileStatus fileStatus = latestfsIterator.next();
        if (fileStatus.isDirectory()) {
          iteratorStack.push(fs.listStatusIterator(fileStatus.getPath()));
          if (Pattern.matches(globPattern, fileStatus.getPath().toUri().getPath())) {
            if (latestfsIterator.hasNext()) {
              // Pushing back the current file status iterator before returning as there are more files to be processed
              iteratorStack.push(latestfsIterator);
            }
            return fileStatus;
          }
        }
      }
    }
    return null;
  }

  /**
   * Converting a globPatter to a regex pattern to match the file status path
   *
   * @param globPattern
   * @return
   */
  private static String getRegexPattern(String globPattern) {
    // Convert the glob pattern to a regex pattern
    String regexPattern = globPattern.replaceAll("\\.", "\\\\.");
    regexPattern = regexPattern.replaceAll("\\*", ".*");
    regexPattern = regexPattern.replaceAll("\\?", ".");
    return regexPattern;
  }

  /**
   * Should return class of T.
   */
  @Override
  public abstract Class<? extends FileSystemDatasetVersion> versionClass();

  /**
   * Glob pattern relative to the root of the dataset used to find {@link org.apache.hadoop.fs.FileStatus} for each
   * dataset version.
   * @return glob pattern relative to dataset root.
   */
  public abstract Path globVersionPattern();

  /**
   * Create a {@link org.apache.gobblin.data.management.version.DatasetVersion} with <code>versionFileStatus</code> and a path
   * relative to the dataset.
   * @param pathRelativeToDatasetRoot {@link org.apache.hadoop.fs.Path} of dataset version relative to dataset root.
   * @param versionFileStatus {@link FileStatus} of the dataset version.
   * @return {@link org.apache.gobblin.data.management.version.DatasetVersion} for that {@link FileStatus}.
   */
  public abstract T getDatasetVersion(Path pathRelativeToDatasetRoot, FileStatus versionFileStatus);

}
