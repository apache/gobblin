package gobblin.writer;

import java.io.OutputStream;

import gobblin.annotation.Alpha;


/**
 * Interface for an object that can encrypt a bytestream.
 */
@Alpha
public interface StreamEncoder {
  /**
   * Wrap a bytestream and return a new stream that when written to
   * encrypts the bytes flowing through.
   * @param origStream Stream to wrap
   * @return A wrapped stream for encryption
   */
  OutputStream wrapOutputStream(OutputStream origStream);

  /**
   * Get tag/file extension associated with encoder
   */
  String getTag();
}
