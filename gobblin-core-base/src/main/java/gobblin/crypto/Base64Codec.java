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
package gobblin.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;

import org.apache.commons.codec.binary.Base64OutputStream;

import gobblin.writer.StreamCodec;

import lombok.extern.slf4j.Slf4j;


/**
 * A class that can encode and decrypt Base64 streams.
 *
 * This class will delegate to the Java 8+ java.util.Base64 algorithm if it can be found;
 * otherwise it relies on Apache Common Codec's Base64OutputStream. The Java 8 classes
 * are preferred because they are noticeably faster in benchmarking.
 */
@Slf4j
public class Base64Codec implements StreamCodec {
  private final static Method java8GetEncoder;
  private final static Method java8WrapStream;
  private static boolean forceApacheBase64 = false;

  @Override
  public OutputStream encodeOutputStream(OutputStream origStream) throws IOException {
    try {
      if (canUseJava8()) {
        Object encoder = java8GetEncoder.invoke(null);
        return (OutputStream) java8WrapStream.invoke(encoder, origStream);
      } else {
        return encodeOutputStreamWithApache(origStream);
      }
    } catch (ReflectiveOperationException e) {
      log.warn("Error invoking java8 methods, falling back to Apache", e);
      return encodeOutputStreamWithApache(origStream);
    }
  }

  @Override
  public InputStream decodeInputStream(InputStream origStream) throws IOException {
    throw new UnsupportedOperationException("not implemented yet");
  }

  static {
    java8GetEncoder = getBase64EncoderMethod();
    java8WrapStream = getBase64EncoderWrapMethod();
    if (java8GetEncoder == null || java8WrapStream == null) {
      log.info("Couldn't find java.util.Base64, falling back to Apache Commons");
    }
  }

  private static Method getBase64EncoderMethod() {
    try {
      Class<?> java8Base64 = Class.forName("java.util.Base64");
      return java8Base64.getMethod("getEncoder");
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      return null;
    }
  }

  private static Method getBase64EncoderWrapMethod() {
    try {
      Class<?> java8Encoder = Class.forName("java.util.Base64$Encoder");
      return java8Encoder.getMethod("wrap", OutputStream.class);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      return null;
    }
  }

  private OutputStream encodeOutputStreamWithApache(OutputStream origStream) {
    return new Base64OutputStream(origStream, true, 0, null);
  }

  // Force use of the Apache Base64 codec -- used only for benchmarking
  static void forceApacheBase64() {
    forceApacheBase64 = true;
  }

  private boolean canUseJava8() {
    return !forceApacheBase64 && java8WrapStream != null && java8GetEncoder != null;
  }

  @Override
  public String getTag() {
    return "base64";
  }
}
