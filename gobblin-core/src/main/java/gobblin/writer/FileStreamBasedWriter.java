package gobblin.writer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import gobblin.configuration.State;
import gobblin.crypto.EncryptionUtils;


/**
 * A base class for writers that write files out to a filesystem using an OutputStream. Handles wrapping
 * encryption support around streams.
 * @param <D>
 */
public abstract class FileStreamBasedWriter<D> extends FsDataWriter<D> {
  static final Set<String> SUPPORTED_ENCRYPTION_TYPES = EncryptionUtils.supportedStreamingAlgorithms();

  protected List<StreamCodec> encoders;

  public FileStreamBasedWriter(FsDataWriterBuilder<?, D> builder, State properties, List<StreamCodec> encoders) throws IOException {
    super(builder, properties);

    this.encoders = encoders;
  }

  /**
   * Create the staging output file and an {@link OutputStream} to write to the file.
   *
   * @return an {@link OutputStream} to write to the staging file
   * @throws IOException if it fails to create the file and the {@link OutputStream}
   */
  protected OutputStream createStagingFileOutputStream()
      throws IOException {
    OutputStream origStream = this.fs
        .create(this.stagingFile, this.filePermission, true, this.bufferSize, this.replicationFactor, this.blockSize,
            null);

    for (StreamCodec e: encoders) {
      origStream = e.wrapOutputStream(origStream);
    }
    return this.closer.register(origStream);
  }
}
