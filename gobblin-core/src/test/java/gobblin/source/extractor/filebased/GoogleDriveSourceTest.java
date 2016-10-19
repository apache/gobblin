package gobblin.source.extractor.filebased;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.google.GoogleDriveExtractor;
import gobblin.source.extractor.extract.google.GoogleDriveFsHelper;
import gobblin.source.extractor.extract.google.GoogleDriveSource;


@Test(groups = { "gobblin.source.extractor.google" })
public class GoogleDriveSourceTest {

  @SuppressWarnings("unchecked")
  public void testGetcurrentFsSnapshot() throws FileBasedHelperException {
    @SuppressWarnings("rawtypes")
    GoogleDriveSource source = new GoogleDriveSource<>();
    GoogleDriveFsHelper fsHelper = mock(GoogleDriveFsHelper.class);
    source.fsHelper = fsHelper;

    List<String> fileIds = ImmutableList.of("test1", "test2", "test3");
    when(fsHelper.ls(anyString())).thenReturn(fileIds);
    long timestamp = System.currentTimeMillis();
    when(fsHelper.getFileMTime(anyString())).thenReturn(timestamp);

    List<String> expected = Lists.newArrayList();
    for (String fileId : fileIds) {
      expected.add(fileId + source.splitPattern + timestamp);
    }

    Assert.assertEquals(expected, source.getcurrentFsSnapshot(new State()));
  }

  public void testGetExtractor() throws IOException {
    @SuppressWarnings("rawtypes")
    GoogleDriveSource source = new GoogleDriveSource<>();
    GoogleDriveFsHelper fsHelper = mock(GoogleDriveFsHelper.class);
    source.fsHelper = fsHelper;
    Extractor extractor = source.getExtractor(new WorkUnitState());

    Assert.assertTrue(extractor instanceof GoogleDriveExtractor);
  }
}
