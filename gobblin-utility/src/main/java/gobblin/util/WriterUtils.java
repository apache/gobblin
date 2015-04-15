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
    Preconditions.checkArgument(state.contains(ForkOperatorUtils.getPropertyNameForBranch(
        ConfigurationKeys.WRITER_STAGING_DIR, numBranches, branchId)), "Missing required property "
        + ConfigurationKeys.WRITER_STAGING_DIR);

    return new Path(state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR,
        numBranches, branchId)), WriterUtils.getWriterFilePath(state, numBranches, branchId));
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
    Preconditions.checkArgument(state.contains(ForkOperatorUtils.getPropertyNameForBranch(
        ConfigurationKeys.WRITER_OUTPUT_DIR, numBranches, branchId)), "Missing required property "
        + ConfigurationKeys.WRITER_OUTPUT_DIR);

    return new Path(state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR,
        numBranches, branchId)), WriterUtils.getWriterFilePath(state, numBranches, branchId));
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
    Preconditions.checkArgument(state.contains(ForkOperatorUtils.getPropertyNameForBranch(
        ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, numBranches, branchId)), "Missing required property "
        + ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR);

    return new Path(state.getProp(ForkOperatorUtils.getPropertyNameForBranch(
        ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, numBranches, branchId)), WriterUtils.getWriterFilePath(state,
        numBranches, branchId));
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
    if (state.contains(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, numBranches,
        branchId))) {
      return new Path(state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH,
          numBranches, branchId)));
    }
    return WriterUtils.getDefaultWriterFilePath(state, numBranches, branchId);
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
    return String.format("%s.%s.%s", state.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_NAME, numBranches, branchId),
        ConfigurationKeys.DEFAULT_WRITER_FILE_NAME), writerId, formatExtension);
  }
}
