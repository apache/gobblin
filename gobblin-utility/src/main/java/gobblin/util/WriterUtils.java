package gobblin.util;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.workunit.WorkUnit;


/**
 * Utility class for use with the {@link gobblin.writer.DataWriter} class.
 */
public class WriterUtils {

  public static Path getWriterStagingDir(State state, int numBranches, int branchId) {
    Preconditions.checkArgument(
        state.contains(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR, numBranches, branchId)),
        "Missing required property " + ConfigurationKeys.WRITER_STAGING_DIR);

    return new Path(state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR, numBranches,
        branchId)), WriterUtils.getWriterFilePath(state, numBranches, branchId));
  }

  public static Path getWriterOutputDir(State state, int numBranches, int branchId) {
    Preconditions.checkArgument(
        state.contains(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, numBranches, branchId)),
        "Missing required property " + ConfigurationKeys.WRITER_OUTPUT_DIR);

    return new Path(state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, numBranches,
        branchId)), WriterUtils.getWriterFilePath(state, numBranches, branchId));
  }

  public static Path getDataPublisherFinalOutputDir(State state, int numBranches, int branchId) {
    Preconditions.checkArgument(
        state.contains(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, numBranches, branchId)),
        "Missing required property " + ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR);

    return new Path(state.getProp(ForkOperatorUtils.getPropertyNameForBranch(
        ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, numBranches, branchId)), WriterUtils.getWriterFilePath(state, numBranches, branchId));
  }

  public static String getWriterFilePath(State state, int numBranches, int branchId) {
    if (state.contains(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, numBranches, branchId))) {
      return state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, numBranches, branchId));
    }
    return WriterUtils.getDefaultWriterFilePath(state, numBranches, branchId);
  }

  public static String getDefaultWriterFilePath(State state, int numBranches, int branchId) {
    if (state instanceof WorkUnitState) {
      WorkUnitState workUnitState = (WorkUnitState) state;
      return ForkOperatorUtils.getPathForBranch(workUnitState, workUnitState.getExtract().getOutputFilePath(), numBranches, branchId);

    } else if (state instanceof WorkUnit) {
      WorkUnit workUnit = (WorkUnit) state;
      return ForkOperatorUtils.getPathForBranch(workUnit, workUnit.getExtract().getOutputFilePath(), numBranches, branchId);
    }

    throw new RuntimeException("In order to get the default value for " + ConfigurationKeys.WRITER_FILE_PATH
        + " the given state must be of type " + WorkUnitState.class.getName() + " or " + WorkUnit.class.getName());
  }

  public static String getWriterFileName(State state, int numBranches, int branchId, String writerId, String formatExtension) {
    return String.format("%s.%s.%s", state.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_NAME, numBranches, branchId),
        ConfigurationKeys.DEFAULT_WRITER_FILE_NAME), writerId, formatExtension);
  }
}
