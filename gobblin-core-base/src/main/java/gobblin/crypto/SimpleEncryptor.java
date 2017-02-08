package gobblin.crypto;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import com.google.common.base.Optional;

import gobblin.configuration.State;
import gobblin.writer.StreamEncoder;


/**
 * Simple encryption algorithm that just increments every byte sent
 * through it by 1. Useful for unit tests or proof of concept, but is not actually secure.
 */
public class SimpleEncryptor implements StreamEncoder {
  public SimpleEncryptor(Map<String, Object> parameters) {
    // SimpleEncryptor doesn't care about parameters
  }

  @Override
  public OutputStream wrapOutputStream(OutputStream origStream) {
    return new FilterOutputStream(origStream) {
      @Override
      public void write(int b) throws IOException {
        out.write((b + 1) % 256);
      }

      @Override
      public void write(byte[] b) throws IOException {
        this.write(b, 0, b.length);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        for (int i = off; i < off + len; i++) {
          this.write(b[i]);
        }
      }

      @Override
      public void close() throws IOException {
        out.close();
      }
    };
  }

  @Override
  public String getTag() {
    return "encrypted_simple";
  }
}
