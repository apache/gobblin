package gobblin.config.configstore.impl;

import java.io.File;
import java.io.IOException;

import com.google.common.io.Files;


public class FilesUtil {

  /**
   * Used to sync two directories recursively
   * @param input - input directory, must be an existing directory
   * @param output - output directory, must be Non existing 
   * @throws IOException
   */
  public static void SyncDirs(File input, File output) throws IOException {
    if (!input.exists())
      return;

    if (output.exists())
      return;

    output.mkdirs();

    if (input.isFile() || output.isFile())
      return;

    File[] children = input.listFiles();
    File tmp;
    for (File f : children) {
      tmp = new File(output, f.getName());
      if (f.isDirectory()) {
        SyncDirs(f, tmp);
      } else {
        Files.copy(f, tmp);
      }

    }
  }
}
