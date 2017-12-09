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

package org.apache.gobblin.data.management.copy.hive;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import org.apache.gobblin.util.PathUtils;


public class HiveTargetPathHelper {

  /**
   * Specifies a root path for the data in a table. All files containing table data will be placed under this directory.
   * <p>
   *   Does some token replacement in the input path. For example, if the table myTable is in DB myDatabase:
   *   /data/$DB/$TABLE -> /data/myDatabase/myTable.
   *   /data/$TABLE -> /data/myTable
   *   /data -> /data/myTable
   * </p>
   *
   * See javadoc for {@link #getTargetPath} for further explanation.
   */
  public static final String COPY_TARGET_TABLE_ROOT = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.table.root";
  /**
   * These two options, in pair, specify the output location of the data files on copy
   * {@link #COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED} specified the prefix of the path (without Scheme and Authority ) to be replaced
   * {@link #COPY_TARGET_TABLE_PREFIX_REPLACEMENT} specified the replacement of {@link #COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED}
   * <p>
   * for example, if the data file is $sourceFs/data/databases/DB/Table/Snapshot/part-00000.avro ,
   * {@link #COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED} is /data/databases
   * {@link #COPY_TARGET_TABLE_PREFIX_REPLACEMENT} is /data/databases/_parallel
   *
   * then, the output location for that file will be
   * $targetFs/data/databases/_parallel/DB/Table/Snapshot/part-00000.avro
   * </p>
   */
  public static final String COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.table.prefixToBeReplaced";
  public static final String COPY_TARGET_TABLE_PREFIX_REPLACEMENT =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.table.prefixReplacement";
  /**
   * Specifies that, on copy, data files for this table should all be relocated to a single directory per partition.
   * See javadoc for {@link #getTargetPath} for further explanation.
   */
  public static final String RELOCATE_DATA_FILES_KEY =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.relocate.data.files";
  public static final String DEFAULT_RELOCATE_DATA_FILES = Boolean.toString(false);
  private final boolean relocateDataFiles;
  private final Optional<Path> targetTableRoot;
  private final Optional<Path> targetTablePrefixTobeReplaced;
  private final Optional<Path> targetTablePrefixReplacement;
  private final HiveDataset dataset;

  public HiveTargetPathHelper(HiveDataset dataset) {

    this.dataset = dataset;
    this.relocateDataFiles = Boolean
        .valueOf(this.dataset.getProperties().getProperty(RELOCATE_DATA_FILES_KEY, DEFAULT_RELOCATE_DATA_FILES));
    this.targetTableRoot = this.dataset.getProperties().containsKey(COPY_TARGET_TABLE_ROOT)
        ? Optional.of(resolvePath(this.dataset.getProperties().getProperty(COPY_TARGET_TABLE_ROOT),
        this.dataset.getTable().getDbName(), this.dataset.getTable().getTableName()))
        : Optional.<Path> absent();

    this.targetTablePrefixTobeReplaced =
        this.dataset.getProperties().containsKey(COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED)
            ? Optional.of(new Path(this.dataset.getProperties().getProperty(COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED)))
            : Optional.<Path> absent();

    this.targetTablePrefixReplacement = this.dataset.getProperties().containsKey(COPY_TARGET_TABLE_PREFIX_REPLACEMENT)
        ? Optional.of(new Path(this.dataset.getProperties().getProperty(COPY_TARGET_TABLE_PREFIX_REPLACEMENT)))
        : Optional.<Path> absent();
  }

  private static Path addPartitionToPath(Path path, Partition partition) {
    for (String partitionValue : partition.getValues()) {
      path = new Path(path, partitionValue);
    }
    return path;
  }

  /**
   * Takes a path with tokens {@link #databaseToken} or {@link #tableToken} and replaces these tokens with the actual
   * database names and table name. For example, if db is myDatabase, table is myTable, then /data/$DB/$TABLE will be
   * resolved to /data/myDatabase/myTable.
   */
  protected static Path resolvePath(String pattern, String database, String table) {
    pattern = pattern.replace(HiveDataset.DATABASE_TOKEN, database);
    if (pattern.contains(HiveDataset.TABLE_TOKEN)) {
      pattern = pattern.replace(HiveDataset.TABLE_TOKEN, table);
      return new Path(pattern);
    } else {
      return new Path(pattern, table);
    }
  }

  /**
   * Compute the target {@link Path} for a file or directory copied by Hive distcp.
   *
   * <p>
   *   The target locations of data files for this table depend on the values of the resolved table root (e.g.
   *   the value of {@link #COPY_TARGET_TABLE_ROOT} with tokens replaced) and {@link #RELOCATE_DATA_FILES_KEY}:
   *   * if {@link #RELOCATE_DATA_FILES_KEY} is true, then origin file /path/to/file/myFile will be written to
   *     /resolved/table/root/<partition>/myFile
   *   * if {@link #COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED} and {@link #COPY_TARGET_TABLE_PREFIX_REPLACEMENT} are defined,
   *     then the specified prefix in each file will be replaced by the specified replacement.
   *   * otherwise, if the resolved table root is defined (e.g. {@link #COPY_TARGET_TABLE_ROOT} is defined in the
   *     properties), we define:
   *     origin_table_root := the deepest non glob ancestor of table.getSc().getLocation() iff getLocation() points to
   *                           a single glob. (e.g. /path/to/*&#47;files -> /path/to). If getLocation() contains none
   *                           or multiple globs, job will fail.
   *     relative_path := path of the file relative to origin_table_root. If the path of the file is not a descendant
   *                      of origin_table_root, job will fail.
   *     target_path := /resolved/table/root/relative/path
   *     This mode is useful when moving a table with a complicated directory structure to a different base directory.
   *   * otherwise the target is identical to the origin path.
   * </p>
   *
   *
   * @param sourcePath Source path to be transformed.
   * @param targetFs target {@link FileSystem}
   * @param partition partition this file belongs to.
   * @param isConcreteFile true if this is a path to an existing file in HDFS.
   */
  public Path getTargetPath(Path sourcePath, FileSystem targetFs, Optional<Partition> partition, boolean isConcreteFile) {
    if (this.relocateDataFiles) {
      Preconditions.checkArgument(this.targetTableRoot.isPresent(), "Must define %s to relocate data files.",
          COPY_TARGET_TABLE_ROOT);
      Path path = this.targetTableRoot.get();
      if (partition.isPresent()) {
        path = addPartitionToPath(path, partition.get());
      }
      if (!isConcreteFile) {
        return targetFs.makeQualified(path);
      }
      return targetFs.makeQualified(new Path(path, sourcePath.getName()));
    }

    // both prefixs must be present as the same time
    // can not used with option {@link #COPY_TARGET_TABLE_ROOT}
    if (this.targetTablePrefixTobeReplaced.isPresent() || this.targetTablePrefixReplacement.isPresent()) {
      Preconditions.checkState(this.targetTablePrefixTobeReplaced.isPresent(),
          String.format("Must specify both %s option and %s option together", COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED,
              COPY_TARGET_TABLE_PREFIX_REPLACEMENT));
      Preconditions.checkState(this.targetTablePrefixReplacement.isPresent(),
          String.format("Must specify both %s option and %s option together", COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED,
              COPY_TARGET_TABLE_PREFIX_REPLACEMENT));

      Preconditions.checkState(!this.targetTableRoot.isPresent(),
          String.format("Can not specify the option %s with option %s ", COPY_TARGET_TABLE_ROOT,
              COPY_TARGET_TABLE_PREFIX_REPLACEMENT));

      Path targetPathWithoutSchemeAndAuthority =
          HiveCopyEntityHelper.replacedPrefix(sourcePath, this.targetTablePrefixTobeReplaced.get(), this.targetTablePrefixReplacement.get());
      return targetFs.makeQualified(targetPathWithoutSchemeAndAuthority);
    } else if (this.targetTableRoot.isPresent()) {
      Preconditions.checkArgument(this.dataset.getTableRootPath().isPresent(),
          "Cannot move paths to a new root unless table has exactly one location.");
      Preconditions.checkArgument(PathUtils.isAncestor(this.dataset.getTableRootPath().get(), sourcePath),
          "When moving paths to a new root, all locations must be descendants of the table root location. "
              + "Table root location: %s, file location: %s.", this.dataset.getTableRootPath(), sourcePath);

      Path relativePath = PathUtils.relativizePath(sourcePath, this.dataset.getTableRootPath().get());
      return targetFs.makeQualified(new Path(this.targetTableRoot.get(), relativePath));
    } else {
      return targetFs.makeQualified(PathUtils.getPathWithoutSchemeAndAuthority(sourcePath));
    }
  }
}
