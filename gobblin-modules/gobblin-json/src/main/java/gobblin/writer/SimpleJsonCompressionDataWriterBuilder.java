package gobblin.writer;

import com.google.gson.JsonElement;

import java.io.IOException;

/**
 * Created by tamasnemeth on 09/11/16.
 */
public class SimpleJsonCompressionDataWriterBuilder extends FsDataWriterBuilder<String, JsonElement>{
  @Override
  public DataWriter<JsonElement> build() throws IOException {
    return new SimpleJsonCompressionDataWriter(this, this.destination.getProperties());
  }
}
