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
package org.apache.gobblin.crypto;

import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;


/**
 * Benchmarks around the RotatingAESEncoder algorithm.
 *
 * It turns out after running some of these that Base64 encoding of the output stream also incurs a large
 * performance cost, so there are benchmarks in here to test the efficacy of the algorithm with a few Base64
 * encoder providers too.
 */
@Fork(3)
public class EncodingBenchmark {
  @State(value = Scope.Benchmark)
  public static class EncodingBenchmarkState {
    public byte[] OneKBytes;

    public SimpleCredentialStore credStore;

    @Setup
    public void setup() throws Exception {
      Random r = new Random();
      OneKBytes = new byte[1024];
      credStore = new SimpleCredentialStore();
      r.nextBytes(OneKBytes);
    }
  }

 @Benchmark
  public byte[] write1KRecordsNewBase64(EncodingBenchmarkState state) throws IOException {
    ByteArrayOutputStream sink = new ByteArrayOutputStream();

    OutputStream os = new RotatingAESCodec(state.credStore).encodeOutputStream(sink);
    os.write(state.OneKBytes);
    os.close();

    return sink.toByteArray();
  }

  @Benchmark
  public byte[] write1KRecordsBase64Only(EncodingBenchmarkState state) throws IOException {
    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    OutputStream os = new Base64OutputStream(sink);
    os.write(state.OneKBytes);
    os.close();

    return sink.toByteArray();
  }

  @Benchmark
  public byte[] write1KRecordsDirectCipherStream(EncodingBenchmarkState state) throws Exception {
    Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
    cipher.init(Cipher.ENCRYPT_MODE, state.credStore.getKey());
    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    OutputStream os = new CipherOutputStream(sink, cipher);
    os.write(state.OneKBytes);
    os.close();

    return sink.toByteArray();
  }

 static class SimpleCredentialStore implements CredentialStore {
    private final SecretKey key;
    private final byte[] keyEncoded;

    public SimpleCredentialStore() {
      SecureRandom r = new SecureRandom();
      byte[] keyBytes = new byte[16];
      r.nextBytes(keyBytes);

      key = new SecretKeySpec(keyBytes, "AES");
      keyEncoded = key.getEncoded();
    }

    @Override
    public byte[] getEncodedKey(String id) {
      if (id.equals("1")) {
        return keyEncoded;
      }

      return null;
    }

    @Override
    public Map<String, byte[]> getAllEncodedKeys() {
      return ImmutableMap.of("1", keyEncoded);
    }

    public SecretKey getKey() {
      return key;
    }
  }
}
