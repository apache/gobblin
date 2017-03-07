package gobblin.compaction.mapreduce;
import gobblin.compaction.dataset.Dataset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.testng.Assert;


import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


/**
 * Test for directory renaming strategy
 * {@link MRCompactor#getDeepestLevelRenamedDirsWithFileExistence(FileSystem, Set)}
 * {@link MRCompactor#getDeepestLevelUnrenamedDirsWithFileExistence(FileSystem, Set)}
 * {@link MRCompactor#renameSourceDirAsCompactionCompete(FileSystem, Dataset)}
 */
@Test(groups = { "gobblin.compaction.mapreduce" })
public class RenameSourceDirectoryTest  {
    private FileSystem fs;
    private static final String RENAME_SRC_DIR = "/tmp/renaming-source-dir";
    private static final String RENAME_SRC_DIR_RUN1_DIR = RENAME_SRC_DIR + "/00_10/run1";
    private static final String RENAME_SRC_DIR_RUN2_DIR = RENAME_SRC_DIR + "/00_10/run2";
    private static final String RENAME_SRC_DIR_RUN3_DIR = RENAME_SRC_DIR + "/10_20/run1";
    private static final String RENAME_SRC_DIR_RUN4_DIR = RENAME_SRC_DIR + "/20_30/run1";
    private static final String RENAME_SRC_DIR_RUN5_DIR = RENAME_SRC_DIR + "/20_30/run2";
    private static final String RENAME_SRC_DIR_RUN4_DIR_COMPLETE = RENAME_SRC_DIR + "/20_30/run2_COMPLETE";
    private static final String RENAME_SRC_DIR_RUN5_DIR_COMPLETE = RENAME_SRC_DIR + "/20_30/run3_COMPLETE";
    private static final String RENAME_SRC_DIR_RUN1_FILE = RENAME_SRC_DIR_RUN1_DIR + "/dummy";
    private static final String RENAME_SRC_DIR_RUN2_FILE = RENAME_SRC_DIR_RUN2_DIR + "/dummy";
    private static final String RENAME_SRC_DIR_RUN3_FILE = RENAME_SRC_DIR_RUN3_DIR + "/dummy";
    private static final String RENAME_SRC_DIR_RUN4_FILE = RENAME_SRC_DIR_RUN4_DIR + "/dummy";
    private static final String RENAME_SRC_DIR_RUN5_FILE = RENAME_SRC_DIR_RUN5_DIR + "/dummy";

    private static final String RENAME_SRC_DIR_RUN4_COMPLETE_FILE = RENAME_SRC_DIR_RUN4_DIR_COMPLETE + "/dummy";
    private static final String RENAME_SRC_DIR_RUN5_COMPLETE_FILE = RENAME_SRC_DIR_RUN5_DIR_COMPLETE + "/dummy";
    @BeforeClass
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        fs = FileSystem.get(conf);
    }

    private void createFile (String path) throws IOException {
        File f = new File(path);
        f.getParentFile().mkdirs();
        f.createNewFile();
    }
    @Test
    public void testUnrenamedDirs() throws Exception {

        fs.delete(new Path(RENAME_SRC_DIR), true);

        createFile(RENAME_SRC_DIR_RUN1_FILE);
        createFile(RENAME_SRC_DIR_RUN2_FILE);
        createFile(RENAME_SRC_DIR_RUN3_FILE);
        createFile(RENAME_SRC_DIR_RUN4_FILE);
        createFile(RENAME_SRC_DIR_RUN5_FILE);

        Set<Path> inputPaths = new HashSet<>();
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN1_DIR));
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN2_DIR));
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN3_DIR));
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN4_DIR));
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN5_DIR));

        Set<Path> unRenamed = MRCompactor.getDeepestLevelUnrenamedDirsWithFileExistence(fs, inputPaths);
        Assert.assertEquals(unRenamed.size(), 5);
        fs.delete(new Path(RENAME_SRC_DIR_RUN1_FILE), false);
        unRenamed = MRCompactor.getDeepestLevelUnrenamedDirsWithFileExistence(fs, inputPaths);
        Assert.assertEquals(unRenamed.size(), 4);
    }

    @Test
    public void testRenamedDirs() throws Exception {

        fs.delete(new Path(RENAME_SRC_DIR), true);

        createFile(RENAME_SRC_DIR_RUN1_FILE);
        createFile(RENAME_SRC_DIR_RUN2_FILE);
        createFile(RENAME_SRC_DIR_RUN3_FILE);
        createFile(RENAME_SRC_DIR_RUN4_COMPLETE_FILE);
        createFile(RENAME_SRC_DIR_RUN5_COMPLETE_FILE);

        Set<Path> inputPaths = new HashSet<>();
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN1_DIR));
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN2_DIR));
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN3_DIR));
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN4_DIR_COMPLETE));
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN5_DIR_COMPLETE));

        Set<Path> renamed = MRCompactor.getDeepestLevelRenamedDirsWithFileExistence(fs, inputPaths);
        Assert.assertEquals(renamed.size(), 2);
        fs.delete(new Path(RENAME_SRC_DIR_RUN1_FILE), false);
        renamed = MRCompactor.getDeepestLevelRenamedDirsWithFileExistence(fs, inputPaths);
        Assert.assertEquals(renamed.size(), 2);
    }


    @Test
    public void testRenamingProcedure() throws Exception {

        fs.delete(new Path(RENAME_SRC_DIR), true);

        createFile(RENAME_SRC_DIR_RUN1_FILE);
        createFile(RENAME_SRC_DIR_RUN2_FILE);
        createFile(RENAME_SRC_DIR_RUN3_FILE);
        createFile(RENAME_SRC_DIR_RUN4_COMPLETE_FILE);
        createFile(RENAME_SRC_DIR_RUN5_COMPLETE_FILE);

        Set<Path> inputPaths = new HashSet<>();
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN1_DIR));
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN2_DIR));
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN3_DIR));
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN4_DIR_COMPLETE));
        inputPaths.add(new Path(RENAME_SRC_DIR_RUN5_DIR_COMPLETE));

        Dataset dataset = mock(Dataset.class);
        Set<Path> unrenamed = MRCompactor.getDeepestLevelUnrenamedDirsWithFileExistence(fs, inputPaths);
        Assert.assertEquals(unrenamed.size(), 3);
        when(dataset.getRenamePaths()).thenReturn(unrenamed);
        MRCompactor.renameSourceDirAsCompactionCompete(fs, dataset);

        Assert.assertEquals(fs.exists(new Path(RENAME_SRC_DIR_RUN1_DIR + "_COMPLETE/dummy")), true);
        Assert.assertEquals(fs.exists(new Path(RENAME_SRC_DIR_RUN2_DIR + "_COMPLETE/dummy")), true);
        Assert.assertEquals(fs.exists(new Path(RENAME_SRC_DIR_RUN3_DIR + "_COMPLETE/dummy")), true);
    }
}
