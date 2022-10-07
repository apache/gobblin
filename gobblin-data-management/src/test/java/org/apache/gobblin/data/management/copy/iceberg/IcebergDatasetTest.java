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

package org.apache.gobblin.data.management.copy.iceberg;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.Data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyContext;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.PreserveAttributes;
import org.testng.collections.Sets;

import static org.mockito.Matchers.any;


/** Tests for {@link org.apache.gobblin.data.management.copy.iceberg.IcebergDataset} */
public class IcebergDatasetTest {

  private static final URI SRC_FS_URI;
  private static final URI DEST_FS_URI;
  static {
    try {
      SRC_FS_URI = new URI("abc", "the.source.org", "/", null);
      DEST_FS_URI = new URI("xyz", "the.dest.org", "/", null);
    } catch (URISyntaxException e) {
      throw new RuntimeException("should not occur!", e);
    }
  }

  private static final String ROOT_PATH = "/root/iceberg/test/";
  private static final String METADATA_PATH = ROOT_PATH + "metadata/metadata.json";
  private static final String MANIFEST_LIST_PATH_0 = ROOT_PATH + "metadata/manifest_list.x";
  private static final String MANIFEST_PATH_0 = ROOT_PATH + "metadata/manifest.a";
  private static final String MANIFEST_DATA_PATH_0A = ROOT_PATH + "data/p0/a";
  private static final String MANIFEST_DATA_PATH_0B = ROOT_PATH + "data/p0/b";
  private static final MockedIcebergTable.SnapshotPaths SNAPSHOT_PATHS_0 = new MockedIcebergTable.SnapshotPaths(
      Optional.of(METADATA_PATH), MANIFEST_LIST_PATH_0,
      Arrays.asList(new IcebergSnapshotInfo.ManifestFileInfo(
          MANIFEST_PATH_0, Arrays.asList(MANIFEST_DATA_PATH_0A, MANIFEST_DATA_PATH_0B)))
  );
  private static final String MANIFEST_LIST_PATH_1 = MANIFEST_LIST_PATH_0.replaceAll("\\.x$", ".y");
  private static final String MANIFEST_PATH_1 = MANIFEST_PATH_0.replaceAll("\\.a$", ".b");
  private static final String MANIFEST_DATA_PATH_1A = MANIFEST_DATA_PATH_0A.replaceAll("/p0/", "/p1/");
  private static final String MANIFEST_DATA_PATH_1B = MANIFEST_DATA_PATH_0B.replaceAll("/p0/", "/p1/");
  private static final MockedIcebergTable.SnapshotPaths SNAPSHOT_PATHS_1 = new MockedIcebergTable.SnapshotPaths(
      Optional.empty(), MANIFEST_LIST_PATH_1,
      Arrays.asList(new IcebergSnapshotInfo.ManifestFileInfo(
          MANIFEST_PATH_1, Arrays.asList(MANIFEST_DATA_PATH_1A, MANIFEST_DATA_PATH_1B)))
  );

  private final String testDbName = "test_db_name";
  private final String testTblName = "test_tbl_name";
  private final Properties copyConfigProperties = new Properties();

  @BeforeClass
  public void setUp() throws Exception {
    copyConfigProperties.setProperty("data.publisher.final.dir", "/test");
  }

  @Test
  public void testGetFilePathsWhenDestEmpty() throws IOException {
    List<IcebergSnapshotInfo> icebergSnapshotInfos = Lists.newArrayList(SNAPSHOT_PATHS_1.asSnapshotInfo(), SNAPSHOT_PATHS_0.asSnapshotInfo());
    List<String> existingDestPaths = Lists.newArrayList();
    Set<Path> expectedResultPaths = withAllSnapshotPaths(Sets.newHashSet(), SNAPSHOT_PATHS_1, SNAPSHOT_PATHS_0);
    validateGetFilePathsGivenDestState(icebergSnapshotInfos, existingDestPaths, expectedResultPaths);
  }

  @Test
  public void testGetFilePathsWhenOneManifestListAtDest() throws IOException {
    List<IcebergSnapshotInfo> icebergSnapshotInfos = Lists.newArrayList(SNAPSHOT_PATHS_1.asSnapshotInfo(), SNAPSHOT_PATHS_0.asSnapshotInfo());
    List<String> existingDestPaths = Lists.newArrayList(MANIFEST_LIST_PATH_1);
    Set<Path> expectedResultPaths = withAllSnapshotPaths(Sets.newHashSet(), SNAPSHOT_PATHS_0);
    validateGetFilePathsGivenDestState(icebergSnapshotInfos, existingDestPaths, expectedResultPaths);
  }

  @Test
  public void testGetFilePathsWhenOneManifestAtDest() throws IOException {
    List<IcebergSnapshotInfo> icebergSnapshotInfos = Lists.newArrayList(SNAPSHOT_PATHS_1.asSnapshotInfo(), SNAPSHOT_PATHS_0.asSnapshotInfo());
    List<String> existingDestPaths = Lists.newArrayList(MANIFEST_PATH_1);
    Set<Path> expectedResultPaths = withAllSnapshotPaths(Sets.newHashSet(), SNAPSHOT_PATHS_0);
    expectedResultPaths.add(new Path(MANIFEST_LIST_PATH_1)); // expect manifest's parent, despite manifest subtree skip
    validateGetFilePathsGivenDestState(icebergSnapshotInfos, existingDestPaths, expectedResultPaths);
  }

  @Test
  public void testGetFilePathsWhenSomeDataFilesAtDest() throws IOException {
    List<IcebergSnapshotInfo> icebergSnapshotInfos = Lists.newArrayList(SNAPSHOT_PATHS_1.asSnapshotInfo(), SNAPSHOT_PATHS_0.asSnapshotInfo());
    List<String> existingDestPaths = Lists.newArrayList(MANIFEST_DATA_PATH_1B, MANIFEST_DATA_PATH_0A);
    Set<Path> expectedResultPaths = withAllSnapshotPaths(withAllSnapshotPaths(Sets.newHashSet(), SNAPSHOT_PATHS_1), SNAPSHOT_PATHS_0);
    expectedResultPaths.remove(new Path(MANIFEST_DATA_PATH_1B));
    expectedResultPaths.remove(new Path(MANIFEST_DATA_PATH_0A));
    validateGetFilePathsGivenDestState(icebergSnapshotInfos, existingDestPaths, expectedResultPaths);
  }

  @Test
  public void testGetFilePathsWillSkipMissingSourceFile() throws IOException {
    List<IcebergSnapshotInfo> icebergSnapshotInfos = Lists.newArrayList(SNAPSHOT_PATHS_1.asSnapshotInfo(), SNAPSHOT_PATHS_0.asSnapshotInfo());
    // pretend this path doesn't exist on source:
    Path missingPath = new Path(MANIFEST_DATA_PATH_0A);
    Set<Path> existingSourcePaths = withAllSnapshotPaths(withAllSnapshotPaths(Sets.newHashSet(), SNAPSHOT_PATHS_1), SNAPSHOT_PATHS_0);
    existingSourcePaths.remove(missingPath);
    List<String> existingDestPaths = Lists.newArrayList(MANIFEST_LIST_PATH_1);
    Set<Path> expectedResultPaths = withAllSnapshotPaths(Sets.newHashSet(), SNAPSHOT_PATHS_0);
    expectedResultPaths.remove(missingPath);
    validateGetFilePathsGivenDestState(
        icebergSnapshotInfos,
        Optional.of(existingSourcePaths.stream().map(Path::toString).collect(Collectors.toList())),
        existingDestPaths,
        expectedResultPaths);
  }

  @Test
  public void testGetFilePathsWhenManifestListsAtDestButNotMetadata() throws IOException {
    List<IcebergSnapshotInfo> icebergSnapshotInfos = Lists.newArrayList(SNAPSHOT_PATHS_1.asSnapshotInfo(), SNAPSHOT_PATHS_0.asSnapshotInfo());
    List<String> existingDestPaths = Lists.newArrayList(MANIFEST_LIST_PATH_1, MANIFEST_LIST_PATH_0);
    Set<Path> expectedResultPaths = Sets.newHashSet();
    expectedResultPaths.add(new Path(METADATA_PATH));
    validateGetFilePathsGivenDestState(icebergSnapshotInfos, existingDestPaths, expectedResultPaths);
  }

  @Test
  public void testGetFilePathsWhenAllAtDest() throws IOException {
    List<IcebergSnapshotInfo> icebergSnapshotInfos = Lists.newArrayList(SNAPSHOT_PATHS_1.asSnapshotInfo(), SNAPSHOT_PATHS_0.asSnapshotInfo());
    List<String> existingDestPaths = Lists.newArrayList(METADATA_PATH, MANIFEST_LIST_PATH_1, MANIFEST_LIST_PATH_0);
    Set<Path> expectedResultPaths = Sets.newHashSet(); // not expecting any delta
    validateGetFilePathsGivenDestState(icebergSnapshotInfos, existingDestPaths, expectedResultPaths);
  }

  /**
   * Test case to generate copy entities for all the file paths for a mocked iceberg table.
   * The assumption here is that we create copy entities for all the matching file paths,
   * without calculating any difference between the source and destination
   */
  @Test
  public void testGenerateCopyEntitiesWhenDestEmpty() throws IOException {
    List<String> expectedPaths = Arrays.asList(METADATA_PATH, MANIFEST_LIST_PATH_0,
        MANIFEST_PATH_0, MANIFEST_DATA_PATH_0A, MANIFEST_DATA_PATH_0B);

    MockFileSystemBuilder sourceBuilder = new MockFileSystemBuilder(SRC_FS_URI);
    sourceBuilder.addPaths(expectedPaths);
    FileSystem sourceFs = sourceBuilder.build();

    IcebergTable icebergTable = new MockedIcebergTable(Arrays.asList(SNAPSHOT_PATHS_0));
    IcebergDataset icebergDataset = new TrickIcebergDataset(testDbName, testTblName, icebergTable, new Properties(), sourceFs);

    MockFileSystemBuilder destBuilder = new MockFileSystemBuilder(DEST_FS_URI);
    FileSystem destFs = destBuilder.build();

    CopyConfiguration copyConfiguration = CopyConfiguration.builder(destFs, copyConfigProperties)
        .preserve(PreserveAttributes.fromMnemonicString(""))
        .copyContext(new CopyContext())
        .build();
    Collection<CopyEntity> copyEntities = icebergDataset.generateCopyEntities(destFs, copyConfiguration);
    verifyCopyEntities(copyEntities, expectedPaths);
  }

  /** Test generating copy entities for a multi-snapshot iceberg; given empty dest, src-dest delta will be entirety */
  @Test
  public void testGenerateCopyEntitiesMultiSnapshotWhenDestEmpty() throws IOException {
    List<String> expectedPaths = Arrays.asList(METADATA_PATH,
        MANIFEST_LIST_PATH_0, MANIFEST_PATH_0, MANIFEST_DATA_PATH_0A, MANIFEST_DATA_PATH_0B,
        MANIFEST_LIST_PATH_1, MANIFEST_PATH_1, MANIFEST_DATA_PATH_1A, MANIFEST_DATA_PATH_1B);

    MockFileSystemBuilder sourceBuilder = new MockFileSystemBuilder(SRC_FS_URI);
    sourceBuilder.addPaths(expectedPaths);
    FileSystem sourceFs = sourceBuilder.build();

    IcebergTable icebergTable = new MockedIcebergTable(Arrays.asList(SNAPSHOT_PATHS_1, SNAPSHOT_PATHS_0));
    IcebergDataset icebergDataset = new TrickIcebergDataset(testDbName, testTblName, icebergTable, new Properties(), sourceFs);

    MockFileSystemBuilder destBuilder = new MockFileSystemBuilder(DEST_FS_URI);
    FileSystem destFs = destBuilder.build();

    CopyConfiguration copyConfiguration = CopyConfiguration.builder(destFs, copyConfigProperties)
        .preserve(PreserveAttributes.fromMnemonicString(""))
        .copyContext(new CopyContext())
        .build();
    Collection<CopyEntity> copyEntities = icebergDataset.generateCopyEntities(destFs, copyConfiguration);
    verifyCopyEntities(copyEntities, expectedPaths);
  }

  /** exercise {@link IcebergDataset::getFilePaths} and validate the result */
  protected void validateGetFilePathsGivenDestState(
      List<IcebergSnapshotInfo> sourceSnapshotInfos,
      List<String> existingDestPaths,
      Set<Path> expectedResultPaths) throws IOException {
    validateGetFilePathsGivenDestState(sourceSnapshotInfos, Optional.empty(),existingDestPaths, expectedResultPaths);
  }

  /** exercise {@link IcebergDataset::getFilePaths} and validate the result */
  protected void validateGetFilePathsGivenDestState(
      List<IcebergSnapshotInfo> sourceSnapshotInfos,
      Optional<List<String>> optExistingSourcePaths,
      List<String> existingDestPaths,
      Set<Path> expectedResultPaths) throws IOException {
    IcebergTable icebergTable = Mockito.mock(IcebergTable.class);
    Mockito.when(icebergTable.getIncrementalSnapshotInfosIterator()).thenReturn(sourceSnapshotInfos.iterator());

    MockFileSystemBuilder sourceFsBuilder = new MockFileSystemBuilder(SRC_FS_URI, !optExistingSourcePaths.isPresent());
    optExistingSourcePaths.ifPresent(sourceFsBuilder::addPaths);
    FileSystem sourceFs = sourceFsBuilder.build();
    IcebergDataset icebergDataset = new IcebergDataset("test_db_name", "test_tbl_name", icebergTable, new Properties(), sourceFs);

    MockFileSystemBuilder destFsBuilder = new MockFileSystemBuilder(DEST_FS_URI);
    destFsBuilder.addPaths(existingDestPaths);
    FileSystem destFs = destFsBuilder.build();
    CopyConfiguration copyConfiguration = CopyConfiguration.builder(destFs, copyConfigProperties)
        .copyContext(new CopyContext())
        .build();

    Map<Path, FileStatus> filePathsToFileStatus = icebergDataset.getFilePathsToFileStatus(destFs, copyConfiguration);
    Assert.assertEquals(filePathsToFileStatus.keySet(), expectedResultPaths);
    // verify solely the path portion of the `FileStatus`, since that's all mock sets up
    Assert.assertEquals(
        filePathsToFileStatus.values().stream().map(FileStatus::getPath).collect(Collectors.toSet()),
        expectedResultPaths);
  }

  /** @return `paths` after adding to it all paths of every one of `snapshotDefs` */
  protected static Set<Path> withAllSnapshotPaths(Set<Path> paths, MockedIcebergTable.SnapshotPaths... snapshotDefs) {
    Arrays.stream(snapshotDefs).flatMap(snapshotDef ->
        snapshotDef.asSnapshotInfo().getAllPaths().stream()
    ).forEach(p ->
        paths.add(new Path(p))
    );
    return paths;
  }


  private static void verifyCopyEntities(Collection<CopyEntity> copyEntities, List<String> expected) {
    List<String> actual = new ArrayList<>();
    for (CopyEntity copyEntity : copyEntities) {
      String json = copyEntity.toString();
      String filepath = new Gson().fromJson(json, JsonObject.class)
          .getAsJsonObject("object-data").getAsJsonObject("origin")
          .getAsJsonObject("object-data").getAsJsonObject("path")
          .getAsJsonObject("object-data").getAsJsonObject("uri")
          .getAsJsonPrimitive("object-data").getAsString();
      actual.add(filepath);
    }
    Assert.assertEquals(actual.size(), expected.size(),
        "Set" + actual.toString() + " vs Set" + expected.toString());
    Assert.assertEqualsNoOrder(actual.toArray(), expected.toArray());
  }


  /**
   *  Sadly, this is needed to avoid losing `FileSystem` mock to replacement from the `FileSystem.get` `static`
   *  Without this, so to lose the mock, we'd be unable to set up any source paths as existing.
   */
  protected static class TrickIcebergDataset extends IcebergDataset {
    public TrickIcebergDataset(String db, String table, IcebergTable icebergTbl, Properties properties, FileSystem sourceFs) {
      super(db, table, icebergTbl, properties, sourceFs);
    }

    @Override // as the `static` is not mock-able
    protected FileSystem getSourceFileSystemFromFileStatus(FileStatus fileStatus, Configuration hadoopConfig) throws IOException {
      return this.sourceFs;
    }
  };


  /** Builds a {@link FileSystem} mock */
  protected static class MockFileSystemBuilder {
    private final URI fsURI;
    /** when not `.isPresent()`, all paths exist; when `.get().isEmpty()`, none exist; else only those indicated do */
    private final Optional<Set<Path>> optPaths;

    public MockFileSystemBuilder(URI fsURI) {
      this(fsURI, false);
    }
    public MockFileSystemBuilder(URI fsURI, boolean shouldRepresentEveryPath) {
      this.fsURI = fsURI;
      this.optPaths = shouldRepresentEveryPath ? Optional.empty() : Optional.of(Sets.newHashSet());
    }

    public Optional<Set<Path>> getPaths() {
      return this.optPaths.map(Sets::newHashSet); // copy before returning
    }

    public void addPaths(List<String> pathStrings) {
      for (String pathString : pathStrings) {
        addPath(pathString);
      }
    }

    public void addPath(String pathString) {
      addPath(new Path(pathString));
    }

    public void addPath(Path path) {
      if (!this.optPaths.isPresent()) {
        throw new IllegalStateException("unable to add paths when constructed with `shouldRepresentEveryPath == true`");
      }
      if (this.optPaths.get().add(path) && !path.isRoot()) { // recursively add ancestors of a previously unknown path
        addPath(path.getParent());
      }
    }

    public FileSystem build() throws IOException {
      FileSystem fs = Mockito.mock(FileSystem.class);
      Mockito.when(fs.getUri()).thenReturn(fsURI);
      Mockito.when(fs.makeQualified(any(Path.class)))
          .thenAnswer(invocation -> invocation.getArgumentAt(0, Path.class).makeQualified(fsURI, new Path("/")));

      if (!this.optPaths.isPresent()) {
        Mockito.when(fs.getFileStatus(any(Path.class))).thenAnswer(invocation ->
            createEmptyFileStatus(invocation.getArgumentAt(0, Path.class).toString()));
      } else {
        // WARNING: order is critical--specific paths *after* `any(Path)`; in addition, since mocking further
        // an already-mocked instance, `.doReturn/.when` is needed (vs. `.when/.thenReturn`)
        Mockito.when(fs.getFileStatus(any(Path.class))).thenThrow(new FileNotFoundException());
        for (Path p : this.optPaths.get()) {
          Mockito.doReturn(createEmptyFileStatus(p.toString())).when(fs).getFileStatus(p);
        }
      }
      return fs;
    }

    protected static FileStatus createEmptyFileStatus(String pathString) throws IOException {
      Path path = new Path(pathString);
      FileStatus fileStatus = new FileStatus();
      fileStatus.setPath(path);
      return fileStatus;
    }
  }


  private static class MockedIcebergTable extends IcebergTable {

    @Data
    public static class SnapshotPaths {
      private final Optional<String> metadataPath;
      private final String manifestListPath;
      private final List<IcebergSnapshotInfo.ManifestFileInfo> manifestFiles;

      public IcebergSnapshotInfo asSnapshotInfo() {
        return asSnapshotInfo(0L, Instant.ofEpochMilli(0L));
      }

      public IcebergSnapshotInfo asSnapshotInfo(Long snapshotId, Instant timestamp) {
        return new IcebergSnapshotInfo(snapshotId, timestamp, this.metadataPath, this.manifestListPath, this.manifestFiles);
      }
    }

    private final List<SnapshotPaths> snapshotPathsList;

    public MockedIcebergTable(List<SnapshotPaths> snapshotPathsList) {
      super(null, null);
      this.snapshotPathsList = Lists.newCopyOnWriteArrayList(snapshotPathsList);
    }

    @Override
    public Iterator<IcebergSnapshotInfo> getAllSnapshotInfosIterator() {
      List<IcebergSnapshotInfo> snapshotInfos = snapshotPathsList.stream()
          .map(SnapshotPaths::asSnapshotInfo)
          .collect(Collectors.toList());
      return snapshotInfos.iterator();
    }
  }
}

