package gobblin.source.extractor.extract.google;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

/**
 * Test for GoogleDriveFileSystemTest. Most of tests are done via @see GoogleDriveFsHelperTest
 *
 */
@Test(groups = { "gobblin.source.extractor.google" })
public class GoogleDriveFileSystemTest {

  public void testQuery() throws IOException {
    GoogleDriveFileSystem fs = new GoogleDriveFileSystem();

    String folderId = "test_folder_id";
    String fileName = "test_file_name";

    Optional<String> query = fs.buildQuery(null, null);
    Assert.assertEquals(query, Optional.absent());

    query = fs.buildQuery(folderId, null);
    Assert.assertEquals(query, Optional.of("'test_folder_id' in parents"));

    query = fs.buildQuery(null, fileName);
    Assert.assertEquals(query, Optional.of("name contains 'test_file_name'"));

    query = fs.buildQuery(folderId, fileName);
    Assert.assertEquals(query, Optional.of("'test_folder_id' in parents and name contains 'test_file_name'"));

    fs.close();
  }

  public void toFileIdTest() {
    String fileId = "test";
    String fileIdWithSlash = "test2/test1";
    String fileIdWithTwoSlashes = "test3/test2/test1";
    String fileIdWithThreeSlashes = "test4/test3/test2/test1";

    Assert.assertEquals(GoogleDriveFileSystem.toFileId(new Path(fileId)), fileId);
    Assert.assertEquals(GoogleDriveFileSystem.toFileId(new Path(fileIdWithSlash)), fileIdWithSlash);
    Assert.assertEquals(GoogleDriveFileSystem.toFileId(new Path(fileIdWithTwoSlashes)), fileIdWithTwoSlashes);
    Assert.assertEquals(GoogleDriveFileSystem.toFileId(new Path(fileIdWithThreeSlashes)), fileIdWithThreeSlashes);
  }
}
