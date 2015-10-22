package gobblin.writer;

import gobblin.configuration.State;
import gobblin.data.management.copy.FileAwareInputStream;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

@Slf4j
public class ArchivedInputStreamDataWriter extends InputStreamDataWriter {

  public ArchivedInputStreamDataWriter(State state, int numBranches, int branchId) throws IOException {
    super(state, numBranches, branchId);
  }

  @Override
  public void write(FileAwareInputStream fileAwareInputStream) throws IOException {
    closer.register(fileAwareInputStream.getInputStream());

    filesWritten++;

    TarArchiveInputStream tarIn = new TarArchiveInputStream(fileAwareInputStream.getInputStream());
    TarArchiveEntry tarEntry;

    String unArchivedRootName = null;

    try {
      while ((tarEntry = tarIn.getNextTarEntry()) != null) {

        Path tarEntryPath = new Path(this.stagingDir, tarEntry.getName());

        if (unArchivedRootName == null) {
          fileAwareInputStream.getFile().setDestination(tarEntryPath);
          unArchivedRootName = tarEntry.getName();
        }

        log.info("Unarchiving " + tarEntryPath);

        if (tarEntry.isDirectory() && !fs.exists(tarEntryPath)) {
          fs.mkdirs(tarEntryPath);
        } else {

          FSDataOutputStream out = fs.create(tarEntryPath, true);
          byte[] btoRead = new byte[1024];
          try {

            int len = 0;

            while ((len = tarIn.read(btoRead)) != -1) {
              bytesWritten += len;
              out.write(btoRead, 0, len);
            }

          } finally {
            out.close();
            btoRead = null;
          }
        }
      }
    } finally {
      tarIn.close();
    }

    this.commit(fileAwareInputStream.getFile());
  }

}
