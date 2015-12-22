package gobblin.applift.simpleconsumer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;

import com.google.common.base.Optional;
import com.google.common.primitives.Longs;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.writer.FsDataWriter;
import gobblin.writer.FsDataWriterBuilder;

public class SimpleStringDataWriter extends FsDataWriter<String> {
  private final Optional<Byte> recordDelimiter; // optional byte to place between each record write
  private int recordsWritten;
  private int bytesWritten;

  private final OutputStream stagingFileOutputStream;
	
	public SimpleStringDataWriter(FsDataWriterBuilder<?, String> builder, State properties) throws IOException {
		super(builder, properties);
		String delim;
    if ((delim = properties.getProp(ConfigurationKeys.SIMPLE_WRITER_DELIMITER, null)) == null || delim.length() == 0) {
      this.recordDelimiter = Optional.absent();
    } else {
      this.recordDelimiter = Optional.of(delim.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)[0]);
    }

    this.recordsWritten = 0;
    this.bytesWritten = 0;
    this.stagingFileOutputStream = createStagingFileOutputStream();
    setStagingFileGroup();
	}

	/*
	 * Change String record to bytes and then write it to HDFS.
	 * @see gobblin.writer.DataWriter#write(java.lang.Object)
	 */
	@Override
	public void write(String record) throws IOException {
		byte[] recordBytes = record.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING);
		byte[] toWrite = recordBytes;
    if (this.recordDelimiter.isPresent()) {
      toWrite = Arrays.copyOf(recordBytes, recordBytes.length + 1);
      toWrite[toWrite.length - 1] = this.recordDelimiter.get();
    }
    this.stagingFileOutputStream.write(toWrite);
    this.bytesWritten += toWrite.length;
    this.recordsWritten++;
	}

	@Override
	public long recordsWritten() {
		return this.recordsWritten();
	}

	@Override
	public long bytesWritten() throws IOException {
		return this.bytesWritten();
	}

}
