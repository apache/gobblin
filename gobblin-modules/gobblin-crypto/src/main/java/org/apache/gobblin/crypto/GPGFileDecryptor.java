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
import java.security.Security;
import java.util.Iterator;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPEncryptedDataList;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPOnePassSignatureList;
import org.bouncycastle.openpgp.PGPPBEEncryptedData;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKeyEncryptedData;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRingCollection;
import org.bouncycastle.openpgp.PGPSignatureList;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.bc.BcKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPDigestCalculatorProviderBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePBEDataDecryptorFactoryBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyDataDecryptorFactoryBuilder;

import lombok.experimental.UtilityClass;

/**
 * A utility class that decrypts both password based and key based encryption files.
 *
 * Code reference - org.bouncycastle.openpgp.examples.PBEFileProcessor
 *                - org.bouncycastle.openpgp.examples.KeyBasedFileProcessor
 */
@UtilityClass
public class GPGFileDecryptor {

  /**
   * Taking in a file inputstream and a passPhrase, generate a decrypted file inputstream.
   * @param inputStream file inputstream
   * @param passPhrase passPhrase
   * @return
   * @throws IOException
   */
  public InputStream decryptFile(InputStream inputStream, String passPhrase) throws IOException {

    PGPEncryptedDataList enc = getPGPEncryptedDataList(inputStream);
    PGPPBEEncryptedData pbe = (PGPPBEEncryptedData) enc.get(0);
    InputStream clear;

    try {
      clear = pbe.getDataStream(new JcePBEDataDecryptorFactoryBuilder(
          new JcaPGPDigestCalculatorProviderBuilder().setProvider(BouncyCastleProvider.PROVIDER_NAME).build())
              .setProvider(BouncyCastleProvider.PROVIDER_NAME).build(passPhrase.toCharArray()));

      JcaPGPObjectFactory pgpFact = new JcaPGPObjectFactory(clear);

      return new LazyMaterializeDecryptorInputStream(pgpFact);
    } catch (PGPException e) {
      throw new IOException(e);
    }
  }

  /**
   * Taking in a file inputstream, keyring inputstream and a passPhrase, generate a decrypted file inputstream.
   * @param inputStream file inputstream
   * @param keyIn keyring inputstream. This InputStream is owned by the caller.
   * @param passPhrase passPhrase
   * @return an {@link InputStream} for the decrypted content
   * @throws IOException
   */
  public InputStream decryptFile(InputStream inputStream, InputStream keyIn, String passPhrase)
      throws IOException {
    try {
      PGPEncryptedDataList enc = getPGPEncryptedDataList(inputStream);
      Iterator it = enc.getEncryptedDataObjects();
      PGPPrivateKey sKey = null;
      PGPPublicKeyEncryptedData pbe = null;
      PGPSecretKeyRingCollection pgpSec = new PGPSecretKeyRingCollection(PGPUtil.getDecoderStream(keyIn), new BcKeyFingerprintCalculator());

      while (sKey == null && it.hasNext()) {
        pbe = (PGPPublicKeyEncryptedData) it.next();
        sKey = findSecretKey(pgpSec, pbe.getKeyID(), passPhrase);
      }

      if (sKey == null) {
        throw new IllegalArgumentException("secret key for message not found.");
      }

      InputStream clear = pbe.getDataStream(
          new JcePublicKeyDataDecryptorFactoryBuilder().setProvider(BouncyCastleProvider.PROVIDER_NAME).build(sKey));
      JcaPGPObjectFactory pgpFact = new JcaPGPObjectFactory(clear);

      return new LazyMaterializeDecryptorInputStream(pgpFact);
    } catch (PGPException e) {
      throw new IOException(e);
    }
  }

  /**
   * Private util function that finds the private key from keyring collection based on keyId and passPhrase
   * @param pgpSec keyring collection
   * @param keyID keyID for this encryption file
   * @param passPhrase passPhrase for this encryption file
   * @throws PGPException
   */
  private PGPPrivateKey findSecretKey(PGPSecretKeyRingCollection pgpSec, long keyID, String passPhrase)
      throws PGPException {

    PGPSecretKey pgpSecKey = pgpSec.getSecretKey(keyID);
    if (pgpSecKey == null) {
      return null;
    }
    return pgpSecKey.extractPrivateKey(
        new JcePBESecretKeyDecryptorBuilder()
            .setProvider(BouncyCastleProvider.PROVIDER_NAME).build(passPhrase.toCharArray()));
  }

  /**
   * Generate a PGPEncryptedDataList from an inputstream
   * @param inputStream file inputstream that needs to be decrypted
   * @throws IOException
   */
  private PGPEncryptedDataList getPGPEncryptedDataList(InputStream inputStream) throws IOException {

    if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
      Security.addProvider(new BouncyCastleProvider());
    }
    inputStream = PGPUtil.getDecoderStream(inputStream);

    JcaPGPObjectFactory pgpF = new JcaPGPObjectFactory(inputStream);
    PGPEncryptedDataList enc;
    Object pgpfObject = pgpF.nextObject();

    if (pgpfObject instanceof PGPEncryptedDataList) {
      enc = (PGPEncryptedDataList) pgpfObject;
    } else {
      enc = (PGPEncryptedDataList) pgpF.nextObject();
    }
    return enc;
  }

  /**
   * A class for reading the underlying {@link InputStream}s from the pgp object without pre-materializing all of them.
   * The PGP object may present the decrypted data through multiple {@link InputStream}s, but these streams are sequential
   * and the n+1 stream is not available until the end of the nth stream is reached, so the
   * {@link LazyMaterializeDecryptorInputStream} keeps a reference to the {@link JcaPGPObjectFactory} and moves to new
   * {@link InputStream}s as they are available
   */
  private static class LazyMaterializeDecryptorInputStream extends InputStream {
    JcaPGPObjectFactory pgpFact;
    InputStream currentUnderlyingStream;

    public LazyMaterializeDecryptorInputStream(JcaPGPObjectFactory pgpFact)
        throws IOException {
      this.pgpFact = pgpFact;

      moveToNextInputStream();
    }

    @Override
    public int read()
        throws IOException {
      if (this.currentUnderlyingStream == null) {
        return -1;
      }

      int value = this.currentUnderlyingStream.read();

      if (value != -1) {
        return value;
      } else {
        moveToNextInputStream();

        if (this.currentUnderlyingStream == null) {
          return -1;
        }

        return this.currentUnderlyingStream.read();
      }
    }

    /**
     * Move to the next {@link InputStream} if available, otherwise set {@link #currentUnderlyingStream} to null to
     * indicate that there is no more data.
     * @throws IOException
     */
    private void moveToNextInputStream() throws IOException {
      Object pgpfObject = this.pgpFact.nextObject();

      // no more data
      if (pgpfObject == null) {
        this.currentUnderlyingStream = null;
        return;
      }

      if (pgpfObject instanceof PGPCompressedData) {
        PGPCompressedData cData = (PGPCompressedData) pgpfObject;

        try {
          this.pgpFact = new JcaPGPObjectFactory(cData.getDataStream());
        } catch (PGPException e) {
          throw new IOException("Could not get the PGP data stream", e);
        }

        pgpfObject = this.pgpFact.nextObject();
      }

      if (pgpfObject instanceof PGPLiteralData) {
        this.currentUnderlyingStream = ((PGPLiteralData) pgpfObject).getInputStream();
      } else if (pgpfObject instanceof PGPOnePassSignatureList) {
        throw new IOException("encrypted message contains PGPOnePassSignatureList message - not literal data.");
      } else if (pgpfObject instanceof PGPSignatureList) {
        throw new IOException("encrypted message contains PGPSignatureList message - not literal data.");
      } else {
        throw new IOException("message is not a simple encrypted file - type unknown.");
      }
    }
  }
}
