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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;

import com.google.common.base.Charsets;
import com.google.common.io.Closer;


/**
 * A utility class for serializing and deserializing Objects to/from Strings.
 *
 * @author ziliu
 */
public class SerializationUtils {

  private static final Charset CHARSET = Charsets.ISO_8859_1;

  /**
   * Serialize an object into a String. The object is first serialized into a byte array,
   * which is converted into a String using charset ISO_8859_1.
   *
   * @param obj A {@link Serializable} object
   * @return A String representing the input object
   */
  public static <T extends Serializable> String serialize(T obj) throws IOException {
    Closer closer = Closer.create();
    try {
      ByteArrayOutputStream bos = closer.register(new ByteArrayOutputStream());
      ObjectOutputStream oos = closer.register(new ObjectOutputStream(bos));
      oos.writeObject(obj);
      oos.flush();
      return bos.toString(CHARSET.name());
    } catch (Throwable e) {
      throw closer.rethrow(e);
    } finally {
      closer.close();
    }
  }

  /**
   * Deserialize a String obtained via {@link #serialize(Serializable)} into an object.
   *
   * @param serialized The serialized String
   * @param clazz The class the deserialized object should be cast to.
   * @return The deserialized object
   */
  public static <T extends Serializable> T deserialize(String serialized, Class<T> clazz) throws IOException {
    Closer closer = Closer.create();
    try {
      ByteArrayInputStream bis = closer.register(new ByteArrayInputStream(serialized.getBytes(CHARSET)));
      ObjectInputStream ois = new ObjectInputStream(bis);
      return clazz.cast(ois.readObject());
    } catch (Throwable e) {
      throw closer.rethrow(e);
    } finally {
      closer.close();
    }
  }
}
