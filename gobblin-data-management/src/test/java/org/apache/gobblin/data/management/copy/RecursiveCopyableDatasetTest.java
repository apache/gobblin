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

package org.apache.gobblin.data.management.copy;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.copy.entities.CommitStepCopyEntity;
import org.apache.gobblin.util.commit.DeleteFileCommitStep;

import avro.shaded.com.google.common.base.Predicate;
import avro.shaded.com.google.common.collect.Iterables;
import javax.annotation.Nullable;
import lombok.Data;


public class RecursiveCopyableDatasetTest {

  @Test
  public void testSimpleCopy() throws Exception {
    Path source = new Path("/source");
    Path target = new Path("/target");

    List<FileStatus> sourceFiles = Lists.newArrayList(createFileStatus(source, "file1"), createFileStatus(source, "file2"));
    List<FileStatus> targetFiles = Lists.newArrayList(createFileStatus(target, "file3"));

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, target.toString());
    RecursiveCopyableDataset dataset = new TestRecursiveCopyableDataset(source, target, sourceFiles, targetFiles, properties);

    Collection<? extends CopyEntity> copyableFiles = dataset.getCopyableFiles(FileSystem.getLocal(new Configuration()),
        CopyConfiguration.builder(FileSystem.getLocal(new Configuration()), properties).build());

    Assert.assertEquals(copyableFiles.size(), 2);
    ClassifiedFiles classifiedFiles = classifyFiles(copyableFiles);
    Assert.assertTrue(classifiedFiles.getPathsToCopy().containsKey(new Path(source, "file1")));
    Assert.assertEquals(classifiedFiles.getPathsToCopy().get(new Path(source, "file1")), new Path(target, "file1"));
    Assert.assertTrue(classifiedFiles.getPathsToCopy().containsKey(new Path(source, "file2")));
    Assert.assertEquals(classifiedFiles.getPathsToCopy().get(new Path(source, "file2")), new Path(target, "file2"));
    Assert.assertEquals(classifiedFiles.getPathsToDelete().size(), 0);
  }

  @Test
  public void testCopyWithNonConflictingCollision() throws Exception {
    Path source = new Path("/source");
    Path target = new Path("/target");

    List<FileStatus> sourceFiles = Lists.newArrayList(createFileStatus(source, "file1", 1), createFileStatus(source, "file2"));
    List<FileStatus> targetFiles = Lists.newArrayList(createFileStatus(target, "file1", 1));

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, target.toString());
    RecursiveCopyableDataset dataset = new TestRecursiveCopyableDataset(source, target, sourceFiles, targetFiles, properties);

    Collection<? extends CopyEntity> copyableFiles = dataset.getCopyableFiles(FileSystem.getLocal(new Configuration()),
        CopyConfiguration.builder(FileSystem.getLocal(new Configuration()), properties).build());

    Assert.assertEquals(copyableFiles.size(), 1);
    ClassifiedFiles classifiedFiles = classifyFiles(copyableFiles);
    Assert.assertTrue(classifiedFiles.getPathsToCopy().containsKey(new Path(source, "file2")));
    Assert.assertEquals(classifiedFiles.getPathsToCopy().get(new Path(source, "file2")), new Path(target, "file2"));
    Assert.assertEquals(classifiedFiles.getPathsToDelete().size(), 0);
  }

  @Test
  public void testCopyWithConflictingCollisionDueToSize() throws Exception {
    Path source = new Path("/source");
    Path target = new Path("/target");

    List<FileStatus> sourceFiles = Lists.newArrayList(createFileStatus(source, "file1", 1), createFileStatus(source, "file2"));
    List<FileStatus> targetFiles = Lists.newArrayList(createFileStatus(target, "file1", 2));

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, target.toString());
    RecursiveCopyableDataset dataset = new TestRecursiveCopyableDataset(source, target, sourceFiles, targetFiles, properties);

    try {
      Collection<? extends CopyEntity> copyableFiles = dataset.getCopyableFiles(FileSystem.getLocal(new Configuration()),
          CopyConfiguration.builder(FileSystem.getLocal(new Configuration()), properties).build());
      Assert.fail();
    } catch (IOException ioe) {
      // should throw exception due to collision
    }
  }

  @Test
  public void testCopyWithConflictingCollisionDueToModtime() throws Exception {
    Path source = new Path("/source");
    Path target = new Path("/target");

    List<FileStatus> sourceFiles = Lists.newArrayList(createFileStatus(source, "file1", 1, 10), createFileStatus(source, "file2"));
    List<FileStatus> targetFiles = Lists.newArrayList(createFileStatus(target, "file1", 1, 9));

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, target.toString());
    RecursiveCopyableDataset dataset = new TestRecursiveCopyableDataset(source, target, sourceFiles, targetFiles, properties);

    try {
      Collection<? extends CopyEntity> copyableFiles = dataset.getCopyableFiles(FileSystem.getLocal(new Configuration()),
          CopyConfiguration.builder(FileSystem.getLocal(new Configuration()), properties).build());
      Assert.fail();
    } catch (IOException ioe) {
      // should throw exception due to collision
    }
  }

  @Test
  public void testCopyWithUpdate() throws Exception {
    Path source = new Path("/source");
    Path target = new Path("/target");

    FileStatus targetFile1 = createFileStatus(target, "file1", 2);

    List<FileStatus> sourceFiles = Lists.newArrayList(createFileStatus(source, "file1", 1), createFileStatus(source, "file2"));
    List<FileStatus> targetFiles = Lists.newArrayList(targetFile1);

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, target.toString());
    properties.setProperty(RecursiveCopyableDataset.UPDATE_KEY, "true");
    RecursiveCopyableDataset dataset = new TestRecursiveCopyableDataset(source, target, sourceFiles, targetFiles, properties);

    FileSystem targetFsUnderlying = FileSystem.getLocal(new Configuration());
    FileSystem targetFs = Mockito.spy(targetFsUnderlying);
    Mockito.doReturn(targetFile1).when(targetFs).getFileStatus(new Path(target, "file1"));

    Collection<? extends CopyEntity> copyableFiles = dataset.getCopyableFiles(targetFs,
        CopyConfiguration.builder(FileSystem.getLocal(new Configuration()), properties).build());

    Assert.assertEquals(copyableFiles.size(), 3);
    ClassifiedFiles classifiedFiles = classifyFiles(copyableFiles);
    Assert.assertTrue(classifiedFiles.getPathsToCopy().containsKey(new Path(source, "file1")));
    Assert.assertEquals(classifiedFiles.getPathsToCopy().get(new Path(source, "file1")), new Path(target, "file1"));
    Assert.assertTrue(classifiedFiles.getPathsToCopy().containsKey(new Path(source, "file2")));
    Assert.assertEquals(classifiedFiles.getPathsToCopy().get(new Path(source, "file2")), new Path(target, "file2"));
    Assert.assertEquals(classifiedFiles.getPathsToDelete().size(), 1);
    Assert.assertTrue(classifiedFiles.getPathsToDelete().contains(new Path(target, "file1")));
  }

  @Test
  public void testCopyWithDeleteTarget() throws Exception {
    Path source = new Path("/source");
    Path target = new Path("/target");

    List<FileStatus> sourceFiles = Lists.newArrayList(createFileStatus(source, "file1"));
    List<FileStatus> targetFiles = Lists.newArrayList(createFileStatus(target, "file3"));

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, target.toString());
    properties.setProperty(RecursiveCopyableDataset.DELETE_KEY, "true");
    RecursiveCopyableDataset dataset = new TestRecursiveCopyableDataset(source, target, sourceFiles, targetFiles, properties);

    Collection<? extends CopyEntity> copyableFiles = dataset.getCopyableFiles(FileSystem.getLocal(new Configuration()),
        CopyConfiguration.builder(FileSystem.getLocal(new Configuration()), properties).build());

    Assert.assertEquals(copyableFiles.size(), 2);
    ClassifiedFiles classifiedFiles = classifyFiles(copyableFiles);
    Assert.assertTrue(classifiedFiles.getPathsToCopy().containsKey(new Path(source, "file1")));
    Assert.assertEquals(classifiedFiles.getPathsToCopy().get(new Path(source, "file1")), new Path(target, "file1"));
    Assert.assertEquals(classifiedFiles.getPathsToDelete().size(), 1);
    Assert.assertTrue(classifiedFiles.getPathsToDelete().contains(new Path(target, "file3")));

    CommitStepCopyEntity entity = (CommitStepCopyEntity) Iterables.filter(copyableFiles, new Predicate<CopyEntity>() {
      @Override
      public boolean apply(@Nullable CopyEntity copyEntity) {
        return copyEntity instanceof CommitStepCopyEntity;
      }
    }).iterator().next();
    DeleteFileCommitStep step = (DeleteFileCommitStep) entity.getStep();
    Assert.assertFalse(step.getParentDeletionLimit().isPresent());
  }

  @Test
  public void testCopyWithDeleteTargetAndDeleteParentDirectories() throws Exception {
    Path source = new Path("/source");
    Path target = new Path("/target");

    List<FileStatus> sourceFiles = Lists.newArrayList(createFileStatus(source, "file1"));
    List<FileStatus> targetFiles = Lists.newArrayList(createFileStatus(target, "file3"));

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, target.toString());
    properties.setProperty(RecursiveCopyableDataset.DELETE_EMPTY_DIRECTORIES_KEY, "true");
    properties.setProperty(RecursiveCopyableDataset.DELETE_KEY, "true");
    RecursiveCopyableDataset dataset = new TestRecursiveCopyableDataset(source, target, sourceFiles, targetFiles, properties);

    Collection<? extends CopyEntity> copyableFiles = dataset.getCopyableFiles(FileSystem.getLocal(new Configuration()),
        CopyConfiguration.builder(FileSystem.getLocal(new Configuration()), properties).build());

    Assert.assertEquals(copyableFiles.size(), 2);
    ClassifiedFiles classifiedFiles = classifyFiles(copyableFiles);
    Assert.assertTrue(classifiedFiles.getPathsToCopy().containsKey(new Path(source, "file1")));
    Assert.assertEquals(classifiedFiles.getPathsToCopy().get(new Path(source, "file1")), new Path(target, "file1"));
    Assert.assertEquals(classifiedFiles.getPathsToDelete().size(), 1);
    Assert.assertTrue(classifiedFiles.getPathsToDelete().contains(new Path(target, "file3")));

    CommitStepCopyEntity entity = (CommitStepCopyEntity) Iterables.filter(copyableFiles, new Predicate<CopyEntity>() {
      @Override
      public boolean apply(@Nullable CopyEntity copyEntity) {
        return copyEntity instanceof CommitStepCopyEntity;
      }
    }).iterator().next();
    DeleteFileCommitStep step = (DeleteFileCommitStep) entity.getStep();
    Assert.assertTrue(step.getParentDeletionLimit().isPresent());
    Assert.assertEquals(step.getParentDeletionLimit().get(), target);
  }

  @Test
  public void testCorrectComputationOfTargetPathsWhenUsingGlob() throws Exception {
    Path source = new Path("/source/directory");
    Path target = new Path("/target");

    List<FileStatus> sourceFiles = Lists.newArrayList(createFileStatus(source, "file1"));
    List<FileStatus> targetFiles = Lists.newArrayList();

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, target.toString());

    FileSystem sourceUnderlyingFS = FileSystem.getLocal(new Configuration());
    FileSystem sourceFs = Mockito.spy(sourceUnderlyingFS);
    Mockito.doReturn(new FileStatus(0, true, 0, 0, 0, source)).when(sourceFs).getFileStatus(source);

    RecursiveCopyableDataset dataset =
        new TestRecursiveCopyableDataset(source, new Path(target, "directory"), sourceFiles, targetFiles, properties,
            new Path("/source/*"), sourceFs);

    Collection<? extends CopyEntity> copyableFiles = dataset.getCopyableFiles(FileSystem.get(new Configuration()),
        CopyConfiguration.builder(FileSystem.getLocal(new Configuration()), properties).build());

    Assert.assertEquals(copyableFiles.size(), 1);
    ClassifiedFiles classifiedFiles = classifyFiles(copyableFiles);
    Assert.assertTrue(classifiedFiles.getPathsToCopy().containsKey(new Path(source, "file1")));
    Assert.assertEquals(classifiedFiles.getPathsToCopy().get(new Path(source, "file1")), new Path(target, "directory/file1"));
  }

  private ClassifiedFiles classifyFiles(Collection<? extends CopyEntity> copyEntities) {
    Map<Path, Path> pathsToCopy = Maps.newHashMap();
    Set<Path> pathsToDelete = Sets.newHashSet();
    for (CopyEntity ce : copyEntities) {
      if (ce instanceof CopyableFile) {
        pathsToCopy.put(((CopyableFile) ce).getOrigin().getPath(), ((CopyableFile) ce).getDestination());
      }
      if (ce instanceof CommitStepCopyEntity) {
        CommitStep step = ((CommitStepCopyEntity) ce).getStep();
        if (step instanceof DeleteFileCommitStep) {
          for (FileStatus status : ((DeleteFileCommitStep) step).getPathsToDelete()) {
            pathsToDelete.add(status.getPath());
          }
        }
      }
    }
    return new ClassifiedFiles(pathsToCopy, pathsToDelete);
  }

  @Data
  private class ClassifiedFiles {
    private final Map<Path, Path> pathsToCopy;
    private final Set<Path> pathsToDelete;
  }

  private FileStatus createFileStatus(Path root, String relative) {
    return createFileStatus(root, relative, 0, 0);
  }

  private FileStatus createFileStatus(Path root, String relative, long length) {
    return createFileStatus(root, relative, length, 0);
  }

  private FileStatus createFileStatus(Path root, String relative, long length, long modtime) {
    return new FileStatus(length, false, 0, 0, modtime, new Path(root, relative));
  }

  private static class TestRecursiveCopyableDataset extends RecursiveCopyableDataset {

    private final Path source;
    private final Path target;
    private final List<FileStatus> sourceFiles;
    private final List<FileStatus> targetFiles;

    public TestRecursiveCopyableDataset(Path source,
        Path target, List<FileStatus> sourceFiles, List<FileStatus> targetFiles, Properties properties) throws IOException {
      this(source, target, sourceFiles, targetFiles, properties, source, FileSystem.getLocal(new Configuration()));
    }

    public TestRecursiveCopyableDataset(Path source, Path target, List<FileStatus> sourceFiles, List<FileStatus> targetFiles,
        Properties properties, Path glob, FileSystem sourceFs) throws IOException {
      super(sourceFs, source, properties, glob);
      this.source = source;
      this.target = target;
      this.sourceFiles = sourceFiles;
      this.targetFiles = targetFiles;
    }

    @Override
    protected List<FileStatus> getFilesAtPath(FileSystem fs, Path path, PathFilter fileFilter)
        throws IOException {
      if (path.equals(this.source)) {
        return this.sourceFiles;
      } else if (path.equals(this.target)) {
        return this.targetFiles;
      } else {
        throw new RuntimeException("Not a recognized path. " + path);
      }
    }
  }
}
