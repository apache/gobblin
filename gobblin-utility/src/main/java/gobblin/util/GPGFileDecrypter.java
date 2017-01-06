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
package gobblin.util;

import gobblin.util.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.security.Security;

import org.apache.hadoop.fs.FSDataInputStream;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPEncryptedDataList;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPPBEEncryptedData;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPDigestCalculatorProviderBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePBEDataDecryptorFactoryBuilder;


/**
 * A utility class that decrypts password based encryption files.
 *
 * Code reference - org.bouncycastle.openpgp.examples.PBEFileProcessor
 */
public class GPGFileDecrypter {

  public static FSDataInputStream decryptFile(InputStream inputStream, String passPhrase) throws IOException {

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

    PGPPBEEncryptedData pbe = (PGPPBEEncryptedData) enc.get(0);

    InputStream clear;
    try {
      clear = pbe.getDataStream(new JcePBEDataDecryptorFactoryBuilder(
          new JcaPGPDigestCalculatorProviderBuilder().setProvider(BouncyCastleProvider.PROVIDER_NAME).build())
              .setProvider(BouncyCastleProvider.PROVIDER_NAME).build(passPhrase.toCharArray()));

      JcaPGPObjectFactory pgpFact = new JcaPGPObjectFactory(clear);
      pgpfObject = pgpFact.nextObject();
      if (pgpfObject instanceof PGPCompressedData) {
        PGPCompressedData cData = (PGPCompressedData) pgpfObject;
        pgpFact = new JcaPGPObjectFactory(cData.getDataStream());
        pgpfObject = pgpFact.nextObject();
      }

      PGPLiteralData ld = (PGPLiteralData) pgpfObject;
      return StreamUtils.convertStream(ld.getInputStream());
    } catch (PGPException e) {
      throw new IOException(e);
    }
  }
}
