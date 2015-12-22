package gobblin.applift.simpleconsumer;

import java.io.IOException;

import gobblin.writer.DataWriter;
import gobblin.writer.FsDataWriterBuilder;

public class SimpleStringDataWriterBuilder extends FsDataWriterBuilder<String, String> {

	@Override
	public DataWriter<String> build() throws IOException {
		return new SimpleStringDataWriter(this, this.destination.getProperties());
	}

}
