package gobblin.crypto;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 * Inverse of InsecureShiftEncryptor - only use is for proof of concept testing - not secure!
 */
public class InsecureShiftDecryptor {
  public InputStream wrapInputStream(InputStream in) {
    return new FilterInputStream(in) {
      @Override
      public int read() throws IOException {
        int upstream = in.read();
        if (upstream == 0) {
          upstream = 255;
        } else if (upstream > 0) {
          upstream--;
        }

        return upstream;
      }

      @Override
      public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        for (int i = 0; i < len; i++) {
          int result = read();
          if (result == -1) {
            return (i == 0) ? -1 : i;
          }

          b[off + i] = (byte) result;
        }

        return len;
      }
    };
  }
}
