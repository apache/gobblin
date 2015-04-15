package gobblin.util;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.source.workunit.Extract.TableType;

/**
 * Tests for {@link WriterUtils}.
 */
@Test(groups = { "gobblin.util" })
public class WriterUtilsTest {

  public static final Path TEST_WRITER_STAGING_DIR = new Path("gobblin-test/writer-staging/");
  public static final Path TEST_WRITER_OUTPUT_DIR = new Path("gobblin-test/writer-output/");
  public static final Path TEST_WRITER_FILE_PATH = new Path("writer/file/path/");
  public static final Path TEST_DATA_PUBLISHER_FINAL_DIR = new Path("writer/final/dir/");

  @Test
  public void testGetWriterDir() {
    State state = new State();

    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, TEST_WRITER_STAGING_DIR);
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, TEST_WRITER_OUTPUT_DIR);
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, TEST_WRITER_FILE_PATH);

    Assert.assertEquals(WriterUtils.getWriterStagingDir(state, 0, 0), new Path(TEST_WRITER_STAGING_DIR,
        TEST_WRITER_FILE_PATH));

    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR + ".0", TEST_WRITER_STAGING_DIR);
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR + ".0", TEST_WRITER_OUTPUT_DIR);
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH + ".0", TEST_WRITER_FILE_PATH);

    Assert.assertEquals(WriterUtils.getWriterStagingDir(state, 2, 0), new Path(TEST_WRITER_STAGING_DIR,
        TEST_WRITER_FILE_PATH));

    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR + ".1", TEST_WRITER_STAGING_DIR);
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR + ".1", TEST_WRITER_OUTPUT_DIR);
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH + ".1", TEST_WRITER_FILE_PATH);

    Assert.assertEquals(WriterUtils.getWriterStagingDir(state, 2, 1), new Path(TEST_WRITER_STAGING_DIR,
        TEST_WRITER_FILE_PATH));
  }

  @Test
  public void testGetDataPublisherFinalOutputDir() {
    State state = new State();

    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, TEST_DATA_PUBLISHER_FINAL_DIR);
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, TEST_WRITER_FILE_PATH);

    Assert.assertEquals(WriterUtils.getDataPublisherFinalDir(state, 0, 0), new Path(TEST_DATA_PUBLISHER_FINAL_DIR,
        TEST_WRITER_FILE_PATH));

    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + ".0", TEST_DATA_PUBLISHER_FINAL_DIR);
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH + ".0", TEST_WRITER_FILE_PATH);

    Assert.assertEquals(WriterUtils.getDataPublisherFinalDir(state, 2, 0), new Path(TEST_DATA_PUBLISHER_FINAL_DIR,
        TEST_WRITER_FILE_PATH));

    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR + ".1", TEST_DATA_PUBLISHER_FINAL_DIR);
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH + ".1", TEST_WRITER_FILE_PATH);

    Assert.assertEquals(WriterUtils.getDataPublisherFinalDir(state, 2, 1), new Path(TEST_DATA_PUBLISHER_FINAL_DIR,
        TEST_WRITER_FILE_PATH));
  }

  @Test
  public void testGetWriterFilePath() {
    WorkUnit state = new WorkUnit();

    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, TEST_WRITER_FILE_PATH);
    Assert.assertEquals(WriterUtils.getWriterFilePath(state, 0, 0), TEST_WRITER_FILE_PATH);

    state.setProp(ConfigurationKeys.WRITER_FILE_PATH + ".0", TEST_WRITER_FILE_PATH);
    Assert.assertEquals(WriterUtils.getWriterFilePath(state, 1, 1), TEST_WRITER_FILE_PATH);
  }

  @Test
  public void testGetDefaultWriterFilePath() {
    String namespace = "gobblin.test";
    String tableName = "test-table";

    SourceState sourceState = new SourceState();
    WorkUnit state = new WorkUnit(sourceState, new Extract(sourceState, TableType.APPEND_ONLY, namespace, tableName));

    Assert.assertEquals(WriterUtils.getWriterFilePath(state, 0, 0), new Path(state.getExtract().getOutputFilePath()));
    Assert.assertEquals(WriterUtils.getWriterFilePath(state, 2, 0), new Path(state.getExtract().getOutputFilePath(),
        ConfigurationKeys.DEFAULT_FORK_BRANCH_NAME + "0"));
  }

  @Test
  public void testGetDefaultWriterFilePathWithWorkUnitState() {
    String namespace = "gobblin.test";
    String tableName = "test-table";

    SourceState sourceState = new SourceState();
    WorkUnit workUnit =
        new WorkUnit(sourceState, new Extract(sourceState, TableType.APPEND_ONLY, namespace, tableName));
    WorkUnitState workUnitState = new WorkUnitState(workUnit);

    Assert.assertEquals(WriterUtils.getWriterFilePath(workUnitState, 0, 0), new Path(workUnitState.getExtract()
        .getOutputFilePath()));
    Assert.assertEquals(WriterUtils.getWriterFilePath(workUnitState, 2, 0), new Path(workUnitState.getExtract()
        .getOutputFilePath(), ConfigurationKeys.DEFAULT_FORK_BRANCH_NAME + "0"));
  }
}
