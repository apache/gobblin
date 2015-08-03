/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Closer;

import gobblin.configuration.State;


/**
 * A utility class for working with Hadoop.
 */
public class HadoopUtils {

  public static Configuration newConfiguration() {
    Configuration conf = new Configuration();

    // Explicitly check for S3 environment variables, so that Hadoop can access s3 and s3n URLs.
    // h/t https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/deploy/SparkHadoopUtil.scala
    String awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
    String awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    if (awsAccessKeyId != null && awsSecretAccessKey != null) {
      conf.set("fs.s3.awsAccessKeyId", awsAccessKeyId);
      conf.set("fs.s3.awsSecretAccessKey", awsSecretAccessKey);
      conf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId);
      conf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey);
    }

    return conf;
  }

  public static List<FileStatus> listStatusRecursive(FileSystem fileSystem, Path path) throws IOException {
    List<FileStatus> results = Lists.newArrayList();
    walk(results, fileSystem, path);
    return results;
  }

  /**
   * A wrapper around this.fs.delete which throws IOException if this.fs.delete returns False.
   * @param f path to be deleted
   * @param recursive whether to do delete sub-directories under the given path (if any) recursively
   * @throws IOException if the deletion fails
   */
  public static void deletePath(FileSystem fs, Path f, boolean recursive) throws IOException {
    if (!fs.delete(f, recursive)) {
      throw new IOException("Failed to delete: " + f);
    }
  }

  public static void renamePath(FileSystem fs, Path oldName, Path newName) throws IOException {
    if (!fs.rename(oldName, newName)) {
      throw new IOException(String.format("Failed to rename %s to %s", oldName.toString(), newName.toString()));
    }
  }

  private static void walk(List<FileStatus> results, FileSystem fileSystem, Path path) throws IOException {
    for (FileStatus status : fileSystem.listStatus(path)) {
      if (!status.isDir()) {
        results.add(status);
      } else {
        walk(results, fileSystem, status.getPath());
      }
    }
  }

  public static Configuration getConfFromState(State state) {
    Configuration conf = new Configuration();
    for (String propName : state.getPropertyNames()) {
      conf.set(propName, state.getProp(propName));
    }
    return conf;
  }

  /**
   * Set the group associated with a given path.
   *
   * @param fs the {@link FileSystem} instance used to perform the file operation
   * @param path the given path
   * @param group the group associated with the path
   * @throws IOException
   */
  public static void setGroup(FileSystem fs, Path path, String group) throws IOException {
    fs.setOwner(path, fs.getFileStatus(path).getOwner(), group);
  }

  /**
   * Serialize a {@link Writable} object into a string.
   *
   * @param writable the {@link Writable} object to be serialized
   * @return a string serialized from the {@link Writable} object
   * @throws IOException if there's something wrong with the serialization
   */
  public static String serializeToString(Writable writable) throws IOException {
    Closer closer = Closer.create();
    try {
      ByteArrayOutputStream byteArrayOutputStream = closer.register(new ByteArrayOutputStream());
      DataOutputStream dataOutputStream = closer.register(new DataOutputStream(byteArrayOutputStream));
      writable.write(dataOutputStream);
      return BaseEncoding.base64().encode(byteArrayOutputStream.toByteArray());
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  /**
   * Deserialize a {@link Writable} object from a string.
   *
   * @param writableClass the {@link Writable} implementation class
   * @param serializedWritableStr the string containing a serialized {@link Writable} object
   * @return a {@link Writable} deserialized from the string
   * @throws IOException if there's something wrong with the deserialization
   */
  public static Writable deserializeFromString(Class<? extends Writable> writableClass, String serializedWritableStr)
      throws IOException {
    return deserializeFromString(writableClass, serializedWritableStr, new Configuration());
  }

  /**
   * Deserialize a {@link Writable} object from a string.
   *
   * @param writableClass the {@link Writable} implementation class
   * @param serializedWritableStr the string containing a serialized {@link Writable} object
   * @param configuration a {@link Configuration} object containing Hadoop configuration properties
   * @return a {@link Writable} deserialized from the string
   * @throws IOException if there's something wrong with the deserialization
   */
  public static Writable deserializeFromString(Class<? extends Writable> writableClass, String serializedWritableStr,
      Configuration configuration) throws IOException {
    Closer closer = Closer.create();
    try {
      byte[] writableBytes = BaseEncoding.base64().decode(serializedWritableStr);
      ByteArrayInputStream byteArrayInputStream = closer.register(new ByteArrayInputStream(writableBytes));
      DataInputStream dataInputStream = closer.register(new DataInputStream(byteArrayInputStream));
      Writable writable = ReflectionUtils.newInstance(writableClass, configuration);
      writable.readFields(dataInputStream);
      return writable;
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }
}
