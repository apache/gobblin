package org.apache.gobblin.util.io;

import java.io.IOException;
import java.io.InputStream;


public class EmptyInputStream extends InputStream {
  public static final InputStream instance = new EmptyInputStream();

  private EmptyInputStream() {}
  @Override
  public int read()
      throws IOException {
    return 0;
  }
}
