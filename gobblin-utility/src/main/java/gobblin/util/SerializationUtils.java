/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
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
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.io.BaseEncoding;
import com.google.common.io.Closer;

import gobblin.configuration.State;


/**
 * A utility class for serializing and deserializing Objects to/from Strings.
 *
 * @author ziliu
 */
public class SerializationUtils {

  private static final BaseEncoding DEFAULT_ENCODING = BaseEncoding.base64();

  /**
   * Serialize an object into a String. The object is first serialized into a byte array,
   * which is converted into a String using {@link BaseEncoding#base64()}.
   *
   * @param obj A {@link Serializable} object
   * @return A String representing the input object
   * @throws IOException if it fails to serialize the object
   */
  public static <T extends Serializable> String serialize(T obj) throws IOException {
    return serialize(obj, DEFAULT_ENCODING);
  }

  /**
   * Serialize an object into a String. The object is first serialized into a byte array,
   * which is converted into a String using the given {@link BaseEncoding}.
   *
   * @param obj A {@link Serializable} object
   * @param enc The {@link BaseEncoding} used to encode a byte array.
   * @return A String representing the input object
   * @throws IOException if it fails to serialize the object
   */
  public static <T extends Serializable> String serialize(T obj, BaseEncoding enc) throws IOException {
    Closer closer = Closer.create();
    try {
      ByteArrayOutputStream bos = closer.register(new ByteArrayOutputStream());
      ObjectOutputStream oos = closer.register(new ObjectOutputStream(bos));
      oos.writeObject(obj);
      oos.flush();
      return enc.encode(bos.toByteArray());
    } catch (Throwable e) {
      throw closer.rethrow(e);
    } finally {
      closer.close();
    }
  }

  /**
   * Deserialize a String obtained via {@link #serialize(Serializable)} into an object, using
   * {@link BaseEncoding#base64()}.
   *
   * @param serialized The serialized String
   * @param clazz The class the deserialized object should be cast to.
   * @return The deserialized object
   * @throws IOException if it fails to deserialize the object
   */
  public static <T extends Serializable> T deserialize(String serialized, Class<T> clazz) throws IOException {
    return deserialize(serialized, clazz, DEFAULT_ENCODING);
  }

  /**
   * Deserialize a String obtained via {@link #serialize(Serializable)} into an object, using the
   * given {@link BaseEncoding}, which must be the same {@link BaseEncoding} used to serialize the object.
   *
   * @param serialized The serialized String
   * @param clazz The class the deserialized object should be cast to.
   * @param enc The {@link BaseEncoding} used to decode the String.
   * @return The deserialized object
   * @throws IOException if it fails to deserialize the object
   */
  public static <T extends Serializable> T deserialize(String serialized, Class<T> clazz, BaseEncoding enc)
      throws IOException {
    Closer closer = Closer.create();
    try {
      ByteArrayInputStream bis = closer.register(new ByteArrayInputStream(enc.decode(serialized)));
      ObjectInputStream ois = closer.register(new ObjectInputStream(bis));
      return clazz.cast(ois.readObject());
    } catch (Throwable e) {
      throw closer.rethrow(e);
    } finally {
      closer.close();
    }
  }

  /**
   * Serialize a {@link State} instance to a file.
   *
   * @param fs the {@link FileSystem} instance for creating the file
   * @param jobStateFilePath the path to the file
   * @param state the {@link State} to serialize
   * @param <T> the {@link State} object type
   * @throws IOException if it fails to serialize the {@link State} instance
   */
  public static <T extends State> void serializeState(FileSystem fs, Path jobStateFilePath, T state)
      throws IOException {
    serializeState(fs, jobStateFilePath, state, fs.getDefaultReplication(jobStateFilePath));
  }

  /**
   * Serialize a {@link State} instance to a file.
   *
   * @param fs the {@link FileSystem} instance for creating the file
   * @param jobStateFilePath the path to the file
   * @param state the {@link State} to serialize
   * @param replication replication of the serialized file.
   * @param <T> the {@link State} object type
   * @throws IOException if it fails to serialize the {@link State} instance
   */
  public static <T extends State> void serializeState(FileSystem fs, Path jobStateFilePath, T state,
      short replication) throws IOException {
    Closer closer = Closer.create();

    try {
      OutputStream os = closer.register(fs.create(jobStateFilePath, replication));
      DataOutputStream dataOutputStream = closer.register(new DataOutputStream(os));
      state.write(dataOutputStream);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  /**
   * Deserialize/read a {@link State} instance from a file.
   *
   * @param fs the {@link FileSystem} instance for opening the file
   * @param jobStateFilePath the path to the file
   * @param state an empty {@link State} instance to deserialize into
   * @param <T> the {@link State} object type
   * @throws IOException if it fails to deserialize the {@link State} instance
   */
  public static <T extends State> void deserializeState(FileSystem fs, Path jobStateFilePath, T state)
      throws IOException {
    Closer closer = Closer.create();

    try {
      InputStream is = closer.register(fs.open(jobStateFilePath));
      DataInputStream dis = closer.register((new DataInputStream(is)));
      state.readFields(dis);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }
}
