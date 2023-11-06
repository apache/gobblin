package org.apache.gobblin.writer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.FileFormatException;
import org.apache.orc.OrcFile;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;

import static org.apache.gobblin.writer.GobblinBaseOrcWriter.CORRUPTED_ORC_FILE_DELETION_EVENT;


public class GobblinBaseOrcWriterTest {

  @Test
  public void testOrcValidation()
      throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    File tmpDir = Files.createTempDir();
    File corruptedOrcFile = new File(tmpDir, "test.orc");
    try (FileWriter writer = new FileWriter(corruptedOrcFile)) {
      // write a corrupted ORC file that only contains the header but without content
      writer.write(OrcFile.MAGIC);
    }

    OrcFile.ReaderOptions readerOptions = new OrcFile.ReaderOptions(conf);

    MetricContext mockContext = Mockito.mock(MetricContext.class);
    Path p = new Path(corruptedOrcFile.getAbsolutePath());
    Assert.assertThrows(FileFormatException.class,
        () -> GobblinBaseOrcWriter.assertOrcFileIsValid(fs, p, readerOptions, mockContext));

    GobblinEventBuilder eventBuilder = new GobblinEventBuilder(CORRUPTED_ORC_FILE_DELETION_EVENT, GobblinBaseOrcWriter.ORC_WRITER_NAMESPACE);
    eventBuilder.addMetadata("filePath", p.toString());
    Mockito.verify(mockContext, Mockito.times(1))
        .submitEvent(eventBuilder.build());
  }
}
