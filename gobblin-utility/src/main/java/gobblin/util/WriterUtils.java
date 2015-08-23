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

package gobblin.util;

import java.io.IOException;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.workunit.WorkUnit;


/**
 * Utility class for use with the {@link gobblin.writer.DataWriter} class.
 */
public class WriterUtils {

  /**
   * TABLENAME should be used for jobs that pull from multiple tables/topics and intend to write the records
   * in each table/topic to a separate folder. Otherwise, DEFAULT can be used.
   */
  public enum WriterFilePathType {
    TABLENAME,
    DEFAULT
  }

  /**
   * Get the {@link Path} corresponding the to the directory a given {@link gobblin.writer.DataWriter} should be writing
   * its staging data. The staging data directory is determined by combining the
   * {@link ConfigurationKeys#WRITER_STAGING_DIR} and the {@link ConfigurationKeys#WRITER_FILE_PATH}.
   * @param state is the {@link State} corresponding to a specific {@link gobblin.writer.DataWriter}.
   * @param numBranches is the total number of branches for the given {@link State}.
   * @param branchId is the id for the specific branch that the {@link gobblin.writer.DataWriter} will write to.
   * @return a {@link Path} specifying the directory where the {@link gobblin.writer.DataWriter} will write to.
   */
  public static Path getWriterStagingDir(State state, int numBranches, int branchId) {
    String writerStagingDirKey =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR, numBranches, branchId);
    Preconditions.checkArgument(state.contains(writerStagingDirKey),
        "Missing required property " + writerStagingDirKey);

    return new Path(
        state.getProp(
            ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR, numBranches, branchId)),
        WriterUtils.getWriterFilePath(state, numBranches, branchId));
  }

  /**
   * Get the {@link Path} corresponding the to the directory a given {@link gobblin.writer.DataWriter} should be writing
   * its output data. The output data directory is determined by combining the
   * {@link ConfigurationKeys#WRITER_OUTPUT_DIR} and the {@link ConfigurationKeys#WRITER_FILE_PATH}.
   * @param state is the {@link State} corresponding to a specific {@link gobblin.writer.DataWriter}.
   * @param numBranches is the total number of branches for the given {@link State}.
   * @param branchId is the id for the specific branch that the {@link gobblin.writer.DataWriter} will write to.
   * @return a {@link Path} specifying the directory where the {@link gobblin.writer.DataWriter} will write to.
   */
  public static Path getWriterOutputDir(State state, int numBranches, int branchId) {
    String writerOutputDirKey =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, numBranches, branchId);
    Preconditions.checkArgument(state.contains(writerOutputDirKey), "Missing required property " + writerOutputDirKey);

    return new Path(
        state.getProp(
            ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, numBranches, branchId)),
        WriterUtils.getWriterFilePath(state, numBranches, branchId));
  }

  /**
   * Get the {@link Path} corresponding the to the directory a given {@link gobblin.publisher.BaseDataPublisher} should
   * commits its output data. The final output data directory is determined by combining the
   * {@link ConfigurationKeys#DATA_PUBLISHER_FINAL_DIR} and the {@link ConfigurationKeys#WRITER_FILE_PATH}.
   * @param state is the {@link State} corresponding to a specific {@link gobblin.writer.DataWriter}.
   * @param numBranches is the total number of branches for the given {@link State}.
   * @param branchId is the id for the specific branch that the {@link gobblin.publisher.BaseDataPublisher} will publish.
   * @return a {@link Path} specifying the directory where the {@link gobblin.publisher.BaseDataPublisher} will publish.
   */
  public static Path getDataPublisherFinalDir(State state, int numBranches, int branchId) {
    String dataPublisherFinalDirKey =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, numBranches, branchId);
    Preconditions.checkArgument(state.contains(dataPublisherFinalDirKey),
        "Missing required property " + dataPublisherFinalDirKey);

    return new Path(state.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, numBranches, branchId)),
        WriterUtils.getWriterFilePath(state, numBranches, branchId));
  }

  /**
   * Get the {@link Path} corresponding the the relative file path for a given {@link gobblin.writer.DataWriter}.
   * This method retrieves the value of {@link ConfigurationKeys#WRITER_FILE_PATH} from the given {@link State}. It also
   * constructs the default value of the {@link ConfigurationKeys#WRITER_FILE_PATH} if not is not specified in the given
   * {@link State}.
   * @param state is the {@link State} corresponding to a specific {@link gobblin.writer.DataWriter}.
   * @param numBranches is the total number of branches for the given {@link State}.
   * @param branchId is the id for the specific branch that the {{@link gobblin.writer.DataWriter} will write to.
   * @return a {@link Path} specifying the relative directory where the {@link gobblin.writer.DataWriter} will write to.
   */
  public static Path getWriterFilePath(State state, int numBranches, int branchId) {
    if (state.contains(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, numBranches, branchId))) {
      return new Path(state.getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, numBranches, branchId)));
    }

    switch (getWriterFilePathType(state)) {
      case TABLENAME:
        return WriterUtils.getTableNameWriterFilePath(state);
      default:
        return WriterUtils.getDefaultWriterFilePath(state, numBranches, branchId);
    }
  }

  private static WriterFilePathType getWriterFilePathType(State state) {
    String pathTypeStr =
        state.getProp(ConfigurationKeys.WRITER_FILE_PATH_TYPE, ConfigurationKeys.DEFAULT_WRITER_FILE_PATH_TYPE);
    return WriterFilePathType.valueOf(pathTypeStr.toUpperCase());
  }

  /**
   * Creates {@link Path} for the {@link ConfigurationKeys#WRITER_FILE_PATH} key according to
   * {@link ConfigurationKeys#EXTRACT_TABLE_NAME_KEY}.
   * @param state
   * @return
   */
  public static Path getTableNameWriterFilePath(State state) {
    Preconditions.checkArgument(state.contains(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY));
    return new Path(state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY));
  }

  /**
   * Creates the default {@link Path} for the {@link ConfigurationKeys#WRITER_FILE_PATH} key.
   * @param numBranches is the total number of branches for the given {@link State}.
   * @param branchId is the id for the specific branch that the {@link gobblin.writer.DataWriter} will write to.
   * @return a {@link Path} specifying the directory where the {@link gobblin.writer.DataWriter} will write to.
   */
  public static Path getDefaultWriterFilePath(State state, int numBranches, int branchId) {
    if (state instanceof WorkUnitState) {
      WorkUnitState workUnitState = (WorkUnitState) state;
      return new Path(ForkOperatorUtils.getPathForBranch(workUnitState, workUnitState.getExtract().getOutputFilePath(),
          numBranches, branchId));

    } else if (state instanceof WorkUnit) {
      WorkUnit workUnit = (WorkUnit) state;
      return new Path(ForkOperatorUtils.getPathForBranch(workUnit, workUnit.getExtract().getOutputFilePath(),
          numBranches, branchId));
    }

    throw new RuntimeException("In order to get the default value for " + ConfigurationKeys.WRITER_FILE_PATH
        + " the given state must be of type " + WorkUnitState.class.getName() + " or " + WorkUnit.class.getName());
  }

  /**
   * Get the value of {@link ConfigurationKeys#WRITER_FILE_NAME} for the a given {@link gobblin.writer.DataWriter}. The
   * method also constructs the default value of the {@link ConfigurationKeys#WRITER_FILE_NAME} if it is not set in the
   * {@link State}
   * @param state is the {@link State} corresponding to a specific {@link gobblin.writer.DataWriter}.
   * @param numBranches is the total number of branches for the given {@link State}.
   * @param branchId is the id for the specific branch that the {{@link gobblin.writer.DataWriter} will write to.
   * @param writerId is the id for a specific {@link gobblin.writer.DataWriter}.
   * @param formatExtension is the format extension for the file (e.g. ".avro").
   * @return a {@link String} representation of the file name.
   */
  public static String getWriterFileName(State state, int numBranches, int branchId, String writerId,
      String formatExtension) {
    return state.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_NAME, numBranches, branchId),
        String.format("%s.%s.%s", ConfigurationKeys.DEFAULT_WRITER_FILE_BASE_NAME, writerId, formatExtension));
  }

  /**
   * Creates a {@link CodecFactory} based on the specified codec name and deflate level. If codecName is absent, then
   * a {@link CodecFactory#deflateCodec(int)} is returned. Otherwise the codecName is converted into a
   * {@link CodecFactory} via the {@link CodecFactory#fromString(String)} method.
   *
   * @param codecName the name of the codec to use (e.g. deflate, snappy, xz, etc.).
   * @param deflateLevel must be an integer from [0-9], and is only applicable if the codecName is "deflate".
   * @return a {@link CodecFactory}.
   */
  public static CodecFactory getCodecFactory(Optional<String> codecName, Optional<String> deflateLevel) {
    if (!codecName.isPresent()) {
      return CodecFactory.deflateCodec(ConfigurationKeys.DEFAULT_DEFLATE_LEVEL);
    } else if (codecName.get().equalsIgnoreCase(DataFileConstants.DEFLATE_CODEC)) {
      if (!deflateLevel.isPresent()) {
        return CodecFactory.deflateCodec(ConfigurationKeys.DEFAULT_DEFLATE_LEVEL);
      } else {
        return CodecFactory.deflateCodec(Integer.parseInt(deflateLevel.get()));
      }
    } else {
      return CodecFactory.fromString(codecName.get());
    }
  }

  /**
   * Create the given dir as well as all missing ancestor dirs. All created dirs will have the given permission.
   * This should be used instead of {@link FileSystem#mkdirs(Path, FsPermission)}, since that method only sets
   * the permission for the given dir, and not recursively for the ancestor dirs.
   *
   * @param fs FileSystem
   * @param path The dir to be created
   * @param perm The permission to be set
   * @throws IOException if failing to create dir or set permission.
   */
  public static void mkdirsWithRecursivePermission(FileSystem fs, Path path, FsPermission perm) throws IOException {
    if (fs.exists(path)) {
      return;
    }
    if (!fs.exists(path.getParent())) {
      mkdirsWithRecursivePermission(fs, path.getParent(), perm);
    }
    if (!fs.mkdirs(path, perm)) {
      throw new IOException(String.format("Unable to mkdir %s with permission %s", path, perm));
    }

    // Double check permission, since fs.mkdirs() may not guarantee to set the permission correctly
    if (!fs.getFileStatus(path).getPermission().equals(perm)) {
      fs.setPermission(path, perm);
    }
  }
}
