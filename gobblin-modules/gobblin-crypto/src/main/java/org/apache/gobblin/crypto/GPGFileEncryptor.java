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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Date;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.operator.bc.BcKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcePBEKeyEncryptionMethodGenerator;
import org.bouncycastle.openpgp.operator.jcajce.JcePGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyKeyEncryptionMethodGenerator;
import org.reflections.ReflectionUtils;

import com.google.common.base.Preconditions;

import lombok.experimental.UtilityClass;


/**
 * A utility class that supports both password based and key based encryption
 *
 * Code reference - org.bouncycastle.openpgp.examples.PBEFileProcessor
 *                - org.bouncycastle.openpgp.examples.KeyBasedFileProcessor
 */
@UtilityClass
public class GPGFileEncryptor {
  private static int BUFFER_SIZE = 1024;
  private static String PAYLOAD_NAME = "payload.file";
  private static String PROVIDER_NAME = BouncyCastleProvider.PROVIDER_NAME;

  /**
   * Taking in an input {@link OutputStream} and a passPhrase, return an {@link OutputStream} that can be used to output
   * encrypted output to the input {@link OutputStream}.
   * @param outputStream the output stream to hold the ciphertext {@link OutputStream}
   * @param passPhrase pass phrase
   * @param cipher the symmetric cipher to use for encryption. If null or empty then a default cipher is used.
   * @return {@link OutputStream} to write content to for encryption
   * @throws IOException
   */
  public OutputStream encryptFile(OutputStream outputStream, String passPhrase, String cipher) throws IOException {
    try {
      if (Security.getProvider(PROVIDER_NAME) == null) {
        Security.addProvider(new BouncyCastleProvider());
      }

      PGPEncryptedDataGenerator cPk = new PGPEncryptedDataGenerator(
          new JcePGPDataEncryptorBuilder(symmetricKeyAlgorithmNameToTag(cipher))
              .setSecureRandom(new SecureRandom())
              .setProvider(PROVIDER_NAME));
      cPk.addMethod(new JcePBEKeyEncryptionMethodGenerator(passPhrase.toCharArray()).setProvider(PROVIDER_NAME));

      OutputStream cOut = cPk.open(outputStream, new byte[BUFFER_SIZE]);

      PGPLiteralDataGenerator literalGen = new PGPLiteralDataGenerator();
      OutputStream _literalOut =
          literalGen.open(cOut, PGPLiteralDataGenerator.BINARY, PAYLOAD_NAME, new Date(), new byte[BUFFER_SIZE]);

      return new ClosingWrapperOutputStream(_literalOut, cOut, outputStream);
    } catch (PGPException e) {
      throw new IOException(e);
    }
  }


  /**
   * Taking in an input {@link OutputStream}, keyring inputstream and a passPhrase, generate an encrypted {@link OutputStream}.
   * @param outputStream {@link OutputStream} that will receive the encrypted content
   * @param keyIn keyring inputstream. This InputStream is owned by the caller.
   * @param keyId key identifier
   * @param cipher the symmetric cipher to use for encryption. If null or empty then a default cipher is used.
   * @return an {@link OutputStream} to write content to for encryption
   * @throws IOException
   */
  public OutputStream encryptFile(OutputStream outputStream, InputStream keyIn, long keyId, String cipher)
      throws IOException {
    try {
      if (Security.getProvider(PROVIDER_NAME) == null) {
        Security.addProvider(new BouncyCastleProvider());
      }

      PGPEncryptedDataGenerator cPk = new PGPEncryptedDataGenerator(
          new JcePGPDataEncryptorBuilder(symmetricKeyAlgorithmNameToTag(cipher))
              .setSecureRandom(new SecureRandom())
              .setProvider(PROVIDER_NAME));

      PGPPublicKey publicKey;
      PGPPublicKeyRingCollection keyRings = new PGPPublicKeyRingCollection(PGPUtil.getDecoderStream(keyIn),
          new BcKeyFingerprintCalculator());
      publicKey = keyRings.getPublicKey(keyId);

      if (publicKey == null) {
        throw new IllegalArgumentException("public key for encryption not found");
      }

      cPk.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(publicKey).setProvider(PROVIDER_NAME));

      OutputStream cOut = cPk.open(outputStream, new byte[BUFFER_SIZE]);

      PGPLiteralDataGenerator literalGen = new PGPLiteralDataGenerator();
      OutputStream _literalOut =
          literalGen.open(cOut, PGPLiteralDataGenerator.BINARY, PAYLOAD_NAME, new Date(), new byte[BUFFER_SIZE]);

      return new ClosingWrapperOutputStream(_literalOut, cOut, outputStream);
    } catch (PGPException e) {
      throw new IOException(e);
    }
  }

  /**
   * Convert a string cipher name to the integer tag used by GPG
   * @param cipherName the cipher name
   * @return integer tag for the cipher
   */
  private static int symmetricKeyAlgorithmNameToTag(String cipherName) {
    // Use CAST5 if no cipher specified
    if (StringUtils.isEmpty(cipherName)) {
      return PGPEncryptedData.CAST5;
    }

    Set<Field> fields = ReflectionUtils.getAllFields(PGPEncryptedData.class, ReflectionUtils.withName(cipherName));

    if (fields.isEmpty()) {
      throw new RuntimeException("Could not find tag for cipher name " + cipherName);
    }

    try {
      return fields.iterator().next().getInt(null);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Could not access field " + cipherName, e);
    }
  }

  /**
   * A class for keeping track of wrapped output streams and closing them when this stream is closed.
   * This is required because GPG wrapping of streams does not propagate the close.
   */
  private static class ClosingWrapperOutputStream extends OutputStream {
    private final OutputStream[] outputStreams;
    private final OutputStream firstStream;

    /**
     * Creates an output stream that writes to the first {@link OutputStream} and closes all of the {@link OutputStream}s
     * when close() is called
     * @param outputStreams list of {@link OutputStream}s where the first one is written to and the rest are tracked
     *                      for closing.
     */
    public ClosingWrapperOutputStream(OutputStream... outputStreams) {
      Preconditions.checkArgument(outputStreams.length >= 1);

      this.outputStreams = outputStreams;
      this.firstStream = outputStreams[0];
    }

    @Override
    public void write(byte[] bytes)
        throws IOException
    {
      this.firstStream.write(bytes);
    }

    @Override
    public void write(byte[] bytes, int offset, int length)
        throws IOException
    {
      this.firstStream.write(bytes, offset, length);
    }

    @Override
    public void write(int b)
        throws IOException
    {
      this.firstStream.write(b);
    }

    public void flush()
        throws IOException
    {
      for (OutputStream os : this.outputStreams) {
        os.flush();
      }
    }

    public void close()
        throws IOException
    {
      for (OutputStream os : this.outputStreams) {
        os.close();
      }
    }
  }
}
