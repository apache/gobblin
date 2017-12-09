/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.util.io;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;


/**
 * Contains utilities for getting uderlying streams to a filter stream.
 */
public class FilterStreamUnpacker {

  /**
   * Finds the underlying {@link InputStream} to a {@link FilterInputStream}. Note this is not always possible due
   * to security restrictions of the JVM.
   * @throws IllegalAccessException If security policies of the JVM prevent unpacking of the {@link FilterInputStream}.
   */
  public static InputStream unpackFilterInputStream(FilterInputStream is) throws IllegalAccessException {
    try {
      Field field = FilterInputStream.class.getDeclaredField("in");
      field.setAccessible(true);
      return (InputStream) field.get(is);
    } catch (NoSuchFieldException nsfe) {
      throw new RuntimeException(nsfe);
    }
  }

  /**
   * Finds the underlying {@link OutputStream} to a {@link FilterOutputStream}. Note this is not always possible due
   * to security restrictions of the JVM.
   * @throws IllegalAccessException If security policies of the JVM prevent unpacking of the {@link FilterOutputStream}.
   */
  public static OutputStream unpackFilterOutputStream(FilterOutputStream os) throws IllegalAccessException {
    try {
      Field field = FilterOutputStream.class.getDeclaredField("out");
      field.setAccessible(true);
      return (OutputStream) field.get(os);
    } catch (NoSuchFieldException nsfe) {
      throw new RuntimeException(nsfe);
    }
  }

}
