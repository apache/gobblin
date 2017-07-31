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
package org.apache.gobblin.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import org.apache.gobblin.compat.hadoop.WritableShim;


/**
 * A serializer that understands how to write objects that implement {@link WritableShim} out to a Hadoop
 * stream. This class must be present in the {@code io.serializations} key of a Hadoop config for the
 * Hadoop runtime to find and instantiate it.
 */
public class WritableShimSerialization implements Serialization<WritableShim> {
  /**
   * Helper method to add this serializer to an existing Hadoop config.
   */
  public static void addToHadoopConfiguration(Configuration conf) {
    final String SERIALIZATION_KEY = "io.serializations";

    String existingSerializers = conf.get(SERIALIZATION_KEY);
    if (existingSerializers != null) {
      conf.set(SERIALIZATION_KEY, existingSerializers + "," + WritableShimSerialization.class.getName());
    } else {
      conf.set(SERIALIZATION_KEY,
          "org.apache.hadoop.io.serializer.WritableSerialization," + WritableShimSerialization.class.getName());
    }
  }

  @Override
  public boolean accept(Class<?> c) {
    return WritableShim.class.isAssignableFrom(c);
  }

  @Override
  public Serializer<WritableShim> getSerializer(Class<WritableShim> c) {
    return new WritableShimSerializer();
  }

  @Override
  public Deserializer<WritableShim> getDeserializer(Class<WritableShim> c) {
    return new WritableShimDeserializer(c);
  }

  private static class WritableShimSerializer implements Serializer<WritableShim> {
    private DataOutputStream out;

    public WritableShimSerializer() {
      out = null;
    }

    @Override
    public void open(OutputStream out)
        throws IOException {
      this.out = new DataOutputStream(out);
    }

    @Override
    public void serialize(WritableShim writableShim)
        throws IOException {
      writableShim.write(this.out);
    }

    @Override
    public void close()
        throws IOException {
      this.out.flush();
      this.out.close();
      this.out = null;
    }
  }

  private static class WritableShimDeserializer implements Deserializer<WritableShim> {
    private final Class<WritableShim> clazz;
    private DataInputStream in;

    public WritableShimDeserializer(Class<WritableShim> c) {
      this.clazz = c;
      this.in = null;
    }

    @Override
    public void open(InputStream in)
        throws IOException {
      this.in = new DataInputStream(in);
    }

    @Override
    public WritableShim deserialize(WritableShim writableShim)
        throws IOException {
      try {
        if (writableShim == null) {
          writableShim = this.clazz.newInstance();
        }

        writableShim.readFields(in);
        return writableShim;
      } catch (ReflectiveOperationException e) {
        throw new IOException("Error creating new object", e);
      }
    }

    @Override
    public void close()
        throws IOException {

    }
  }
}
