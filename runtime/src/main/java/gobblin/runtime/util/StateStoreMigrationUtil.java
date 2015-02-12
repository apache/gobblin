/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.io.Closer;

import gobblin.configuration.State;
import gobblin.runtime.JobState;
import gobblin.runtime.TaskState;


/**
 * A utility for state store migration due to changes such as package renaming.
 *
 * @author ynli
 */
public class StateStoreMigrationUtil {

  /**
   * Migrate state files with a given extension from an old state class to a new state class.
   *
   * @param fsUri the file system URI used by the state store
   * @param srcDir source state store root directory
   * @param destDir destination task state store directory
   * @param oldStateClassName the fully-qualified name of the old state class
   * @param newStateClass the new state class
   * @param stateFileExtension the state file extension
   * @throws IOException
   */
  public static void migrate(String fsUri, String srcDir, String destDir, String oldStateClassName,
      Class<? extends State> newStateClass, String stateFileExtension)
      throws ClassNotFoundException, IOException {

    @SuppressWarnings("unchecked")
    Class<? extends Writable> oldStateClass = (Class<? extends Writable>) Class.forName(oldStateClassName);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(fsUri), conf);

    FileStatus[] srcStores = fs.listStatus(new Path(srcDir));
    if (srcStores == null || srcStores.length == 0) {
      System.err.println("No state stores found under " + srcDir);
      return;
    }

    for (FileStatus srcStore : srcStores) {
      System.out.println("Processing source state store " + srcStore.getPath());

      Path destStore = new Path(destDir, srcStore.getPath().getName());
      if (!fs.exists(destStore)) {
        fs.mkdirs(destStore);
      }

      FileStatus[] srcStoreFiles = fs.listStatus(srcStore.getPath(), new StateStoreFileFilter(stateFileExtension));
      if (srcStoreFiles == null || srcStoreFiles.length == 0) {
        System.err.println("No state files found under " + srcStore.getPath());
        continue;
      }

      for (FileStatus srcStoreFile : srcStoreFiles) {
        System.out.println("\tProcessing source state file " + srcStoreFile.getPath());

        Closer closer = Closer.create();
        try {
          // Reader for the old state file
          SequenceFile.Reader reader = closer.register(new SequenceFile.Reader(fs, srcStoreFile.getPath(), conf));
          Path destStoreFile = new Path(destStore, srcStoreFile.getPath().getName());
          // Writer for the new state file
          SequenceFile.Writer writer =
              closer.register(new SequenceFile.Writer(fs, conf, destStoreFile, Text.class, newStateClass));

          Text key = new Text();
          // A state object of the old state class to read in state values of the old state class
          Writable value = ReflectionUtils.newInstance(oldStateClass, conf);

          while (reader.next(key, value)) {
            // Serializing the state object of the old class into a byte array
            ByteArrayOutputStream byteArrayOutputStream = closer.register(new ByteArrayOutputStream());
            DataOutputStream dataOutputStream = closer.register(new DataOutputStream(byteArrayOutputStream));
            value.write(dataOutputStream);

            // Deserialize the byte array into a state object of the new class
            State newValue = ReflectionUtils.newInstance(newStateClass, conf);
            ByteArrayInputStream byteArrayInputStream =
                closer.register((new ByteArrayInputStream(byteArrayOutputStream.toByteArray())));
            DataInputStream dataInputStream = closer.register((new DataInputStream(byteArrayInputStream)));
            newValue.readFields(dataInputStream);

            // Write out the new state object
            writer.append(key, newValue);

            // We are not reusing the value object since it has a Properties instance inside
            // that may not have a clean state before the deserialization
            value = ReflectionUtils.newInstance(oldStateClass, conf);
          }
        } catch (Throwable t) {
          throw closer.rethrow(t);
        } finally {
          closer.close();
        }
      }
    }
  }

  /**
   * A custom {@link org.apache.hadoop.fs.PathFilter} for selecting state files with a given extension.
   */
  private static class StateStoreFileFilter implements PathFilter {

    private final String extension;

    public StateStoreFileFilter(String extension) {
      this.extension = extension;
    }

    @Override
    public boolean accept(Path path) {
      return path.getName().endsWith(this.extension);
    }
  }

  public static void main(String[] args) throws ClassNotFoundException, IOException {
    if (args.length != 3) {
      System.err.println("Usage: StateStoreMigrationUtil <fs uri> <src store root dir> <dest store root dir>");
      System.exit(1);
    }

    // Migrate both task and job state files
    migrate(args[0], args[1], args[2], "com.linkedin.uif.runtime.TaskState", TaskState.class, ".tst");
    migrate(args[0], args[1], args[2], "com.linkedin.uif.runtime.JobState", JobState.class, ".jst");
  }
}
