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

  public static Path getWriterStagingDir(State state, int branch) {
    Preconditions.checkArgument(
        state.contains(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR, branch)),
        "Missing required property " + ConfigurationKeys.WRITER_STAGING_DIR);

    return new Path(state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR,
        branch)), WriterUtils.getWriterFilePath(state, branch));
  }

  public static Path getWriterOutputDir(State state, int branch) {
    Preconditions.checkArgument(
        state.contains(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, branch)),
        "Missing required property " + ConfigurationKeys.WRITER_OUTPUT_DIR);

    return new Path(state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR,
        branch)), WriterUtils.getWriterFilePath(state, branch));
  }

  public static Path getDataPublisherFinalOutputDir(State state, int branch) {
    Preconditions.checkArgument(
        state.contains(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, branch)),
        "Missing required property " + ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR);

    return new Path(state.getProp(ForkOperatorUtils.getPropertyNameForBranch(
        ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, branch)), WriterUtils.getWriterFilePath(state, branch));
  }

  public static String getWriterFilePath(State state, int branch) {
    if (state.contains(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, branch))) {
      return state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, branch));
    }
    return WriterUtils.getDefaultWriterFilePath(state, branch);
  }

  public static String getDefaultWriterFilePath(State state, int branch) {
    if (state instanceof WorkUnitState) {
      WorkUnitState workUnitState = (WorkUnitState) state;
      return ForkOperatorUtils.getPathForBranch(workUnitState, workUnitState.getExtract().getOutputFilePath(), branch);

    } else if (state instanceof WorkUnit) {
      WorkUnit workUnit = (WorkUnit) state;
      return ForkOperatorUtils.getPathForBranch(workUnit, workUnit.getExtract().getOutputFilePath(), branch);
    }

    throw new RuntimeException("In order to get the default value for " + ConfigurationKeys.WRITER_FILE_PATH
        + " the given state must be of type " + WorkUnitState.class.getName() + " or " + WorkUnit.class.getName());
  }

  public static String getWriterFileName(State state, int branch, String writerId, String formatExtension) {
    return String.format("%s.%s.%s", state.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_NAME, branch),
        ConfigurationKeys.DEFAULT_WRITER_FILE_NAME), writerId, formatExtension);
  }
}
