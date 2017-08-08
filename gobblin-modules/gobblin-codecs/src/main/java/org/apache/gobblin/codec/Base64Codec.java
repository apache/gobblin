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
package org.apache.gobblin.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;

import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that can encode and decrypt Base64 streams.
 *
 * This class will delegate to the Java 8+ java.util.Base64 algorithm if it can be found;
 * otherwise it relies on Apache Common Codec's Base64OutputStream. The Java 8 classes
 * are preferred because they are noticeably faster in benchmarking.
 */
public class Base64Codec implements StreamCodec {
  private static final Logger log = LoggerFactory.getLogger(Base64Codec.class);

  private static Method java8GetEncoder;
  private static Method java8WrapStreamEncode;
  private static Method java8GetDecoder;
  private static Method java8WrapStreamDecode;

  private final static boolean foundJava8;

  private static boolean forceApacheBase64 = false;

  @Override
  public OutputStream encodeOutputStream(OutputStream origStream)
      throws IOException {
    try {
      if (canUseJava8()) {
        Object encoder = java8GetEncoder.invoke(null);
        return (OutputStream) java8WrapStreamEncode.invoke(encoder, origStream);
      } else {
        return encodeOutputStreamWithApache(origStream);
      }
    } catch (ReflectiveOperationException e) {
      log.warn("Error invoking java8 methods, falling back to Apache", e);
      return encodeOutputStreamWithApache(origStream);
    }
  }

  @Override
  public InputStream decodeInputStream(InputStream origStream)
      throws IOException {
    try {
      if (canUseJava8()) {
        Object decoder = java8GetDecoder.invoke(null);
        return (InputStream) java8WrapStreamDecode.invoke(decoder, origStream);
      } else {
        return decodeInputStreamWithApache(origStream);
      }
    } catch (ReflectiveOperationException e) {
      log.warn("Error invoking java8 methods, falling back to Apache", e);
      return decodeInputStreamWithApache(origStream);
    }
  }

  static {
    boolean base64Found = false;
    try {
      Class.forName("java.util.Base64");

      java8GetEncoder = getMethod("java.util.Base64", "getEncoder");
      java8WrapStreamEncode = getMethod("java.util.Base64$Encoder", "wrap", OutputStream.class);
      java8GetDecoder = getMethod("java.util.Base64", "getDecoder");
      java8WrapStreamDecode = getMethod("java.util.Base64$Decoder", "wrap", InputStream.class);
      base64Found = true;
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      log.info("Couldn't find java.util.Base64 or methods, falling back to Apache Commons", e);
      base64Found = false;
    } finally {
      foundJava8 = base64Found;
    }
  }

  private static Method getMethod(String className, String methodName, Class<?>... parameterTypes)
      throws ClassNotFoundException, NoSuchMethodException {
    Class<?> clazz = Class.forName(className);
    return clazz.getMethod(methodName, parameterTypes);
  }

  private OutputStream encodeOutputStreamWithApache(OutputStream origStream) {
    return new Base64OutputStream(origStream, true, 0, null);
  }

  private InputStream decodeInputStreamWithApache(InputStream origStream) {
    return new Base64InputStream(origStream);
  }

  // Force use of the Apache Base64 codec -- used only for benchmarking
  static void forceApacheBase64() {
    forceApacheBase64 = true;
  }

  private boolean canUseJava8() {
    return !forceApacheBase64 && foundJava8;
  }

  @Override
  public String getTag() {
    return "base64";
  }
}
