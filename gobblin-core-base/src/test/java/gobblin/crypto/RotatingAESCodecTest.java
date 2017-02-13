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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;


public class RotatingAESCodecTest {
  @Test
  public void testStream() throws IOException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException,
                           InvalidAlgorithmParameterException{
    final byte[] toWrite = "hello world".getBytes();

    SimpleCredentialStore credStore = new SimpleCredentialStore();
    RotatingAESCodec encryptor = new RotatingAESCodec(credStore);
    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    OutputStream os = encryptor.encodeOutputStream(sink);
    os.write(toWrite);
    os.close();

    InputStream in = new ByteArrayInputStream(sink.toByteArray());
    verifyKeyId(in, 1);

    Integer ivLen = verifyIvLen(in);

    byte[] ivBinary = verifyAndExtractIv(in, ivLen);

    byte[] body = readAndBase64DecodeBody(in);

    // feed back into cipheroutput stream
    Cipher inputCipher =  Cipher.getInstance("AES/CBC/PKCS5Padding");
    IvParameterSpec ivParameterSpec =  new IvParameterSpec(ivBinary);
    inputCipher.init(Cipher.DECRYPT_MODE, credStore.getKey(), ivParameterSpec);

    CipherInputStream cis = new CipherInputStream(new ByteArrayInputStream(body), inputCipher);
    byte[] decoded = IOUtils.toByteArray(cis);
    Assert.assertEquals(decoded, toWrite, "Expected decoded output to match encoded output");
  }

  @Test
  public void testLotsOfData() throws Exception {
    long bytesToWrite = 50*1000*1000;
    byte[] buf = new byte[16384];
    long bytesWritten = 0;
    SimpleCredentialStore credStore = new SimpleCredentialStore();
    RotatingAESCodec encryptor = new RotatingAESCodec(credStore);
    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    ByteArrayOutputStream originalBytes = new ByteArrayOutputStream();
    OutputStream encryptedStream = encryptor.encodeOutputStream(sink);
    Random r = new Random();

    while (bytesWritten < bytesToWrite) {
      r.nextBytes(buf);
      originalBytes.write(buf);
      encryptedStream.write(buf);
      bytesWritten += buf.length;
    }

    encryptedStream.close();

    InputStream in = new ByteArrayInputStream(sink.toByteArray());
    verifyKeyId(in, 1);

    Integer ivLen = verifyIvLen(in);

    byte[] ivBinary = verifyAndExtractIv(in, ivLen);

    byte[] body = readAndBase64DecodeBody(in);

    // feed back into cipheroutput stream
    Cipher inputCipher =  Cipher.getInstance("AES/CBC/PKCS5Padding");
    IvParameterSpec ivParameterSpec =  new IvParameterSpec(ivBinary);
    inputCipher.init(Cipher.DECRYPT_MODE, credStore.getKey(), ivParameterSpec);

    CipherInputStream cis = new CipherInputStream(new ByteArrayInputStream(body), inputCipher);
    byte[] decoded = IOUtils.toByteArray(cis);
    Assert.assertEquals(decoded, originalBytes.toByteArray(), "Expected decoded output to match encoded output");
  }

  private byte[] readAndBase64DecodeBody(InputStream in) throws IOException {
    byte[] body = IOUtils.toByteArray(in);
    body = DatatypeConverter.parseBase64Binary(new String(body, "UTF-8"));
    return body;
  }

  private byte[] verifyAndExtractIv(InputStream in, Integer ivLen) throws IOException {
    int bytesRead;
    byte[] base64Iv = new byte[ivLen];
    bytesRead = in.read(base64Iv);
    Assert.assertEquals(Integer.valueOf(bytesRead), Integer.valueOf(ivLen), "Expected to read IV");
    return DatatypeConverter.parseBase64Binary(new String(base64Iv, "UTF-8"));
  }

  private Integer verifyIvLen(InputStream in) throws IOException {
    int bytesRead;
    byte[] ivLenBytes = new byte[3];
    bytesRead = in.read(ivLenBytes);
    Assert.assertEquals(bytesRead, ivLenBytes.length, "Expected to be able to iv length");
    Integer ivLen = Integer.valueOf(new String(ivLenBytes, "UTF-8"));
    Assert.assertEquals(Integer.valueOf(ivLen), Integer.valueOf(24), "Always expect IV to be 24 bytes base64 encoded");
    return ivLen;
  }

  private void verifyKeyId(InputStream in, int expectedKeyId) throws IOException {
    // Verify keyId is properly padded
    byte[] keyIdBytes = new byte[4];
    int bytesRead = in.read(keyIdBytes);
    Assert.assertEquals(bytesRead, keyIdBytes.length, "Expected to be able to read key id");
    String keyId = new String(keyIdBytes, "UTF-8");
    Assert.assertEquals(Integer.valueOf(keyId), Integer.valueOf(expectedKeyId), "Expected keyId to equal 1");
  }

  static class SimpleCredentialStore implements CredentialStore {
    private final SecretKey key;

    public SimpleCredentialStore() {
      SecureRandom r = new SecureRandom();
      byte[] keyBytes = new byte[16];
      r.nextBytes(keyBytes);

      key = new SecretKeySpec(keyBytes, "AES");
    }

    @Override
    public byte[] getEncodedKey(String id) {
      if (id.equals("1")) {
        return key.getEncoded();
      }

      return null;
    }

    @Override
    public Map<String, byte[]> getAllEncodedKeys() {
      return ImmutableMap.of("1", key.getEncoded());
    }

    public SecretKey getKey() {
      return key;
    }
  }
}
