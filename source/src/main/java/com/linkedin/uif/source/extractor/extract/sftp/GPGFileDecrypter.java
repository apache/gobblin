package com.linkedin.uif.source.extractor.extract.sftp;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPEncryptedDataList;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPOnePassSignatureList;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKeyEncryptedData;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRingCollection;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.operator.bc.BcPBESecretKeyDecryptorBuilder;
import org.bouncycastle.openpgp.operator.bc.BcPGPDigestCalculatorProvider;
import org.bouncycastle.openpgp.operator.bc.BcPublicKeyDataDecryptorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to decrypt GPG files using the BouncyCastle API
 * @author stakiar
 */
public class GPGFileDecrypter
{
    private static final Logger log = LoggerFactory.getLogger(ResponsysExtractor.class);
    
    public static InputStream decryptGPGFile(InputStream gpgFile, String privateKeyFile) throws IOException
    {
        InputStream fileIn = gpgFile;
        InputStream keyIn = new BufferedInputStream(new FileInputStream(privateKeyFile));

        fileIn = PGPUtil.getDecoderStream(fileIn);
        InputStream unc = null;

        try
        {
            PGPObjectFactory pgpF = new PGPObjectFactory(fileIn);
            PGPEncryptedDataList enc;
            Object o = pgpF.nextObject();

            // The first object might be a PGP marker packet
            if (o instanceof PGPEncryptedDataList)
            {
                enc = (PGPEncryptedDataList) o;
            }
            else
            {
                enc = (PGPEncryptedDataList) pgpF.nextObject();
            }

            @SuppressWarnings("unchecked")
            Iterator<PGPPublicKeyEncryptedData> it = (Iterator<PGPPublicKeyEncryptedData>) enc.getEncryptedDataObjects();
            PGPPrivateKey sKey = null;
            PGPPublicKeyEncryptedData pbe = null;
            PGPSecretKeyRingCollection pgpSec =
                    new PGPSecretKeyRingCollection(PGPUtil.getDecoderStream(keyIn));
            while (sKey == null && it.hasNext())
            {
                pbe = (PGPPublicKeyEncryptedData) it.next();

                PGPSecretKey pgpSecKey = pgpSec.getSecretKey(pbe.getKeyID());
                sKey = pgpSecKey.extractPrivateKey(new BcPBESecretKeyDecryptorBuilder(new BcPGPDigestCalculatorProvider()).build(null));
            }

            if (sKey == null)
            {
                throw new IllegalArgumentException("Secret key for message not found");
            }

            InputStream clear = pbe.getDataStream(new BcPublicKeyDataDecryptorFactory(sKey));
            PGPObjectFactory plainFact = new PGPObjectFactory(clear);
            Object message = plainFact.nextObject();

            if (message instanceof PGPCompressedData)
            {
                PGPCompressedData cData = (PGPCompressedData) message;
                PGPObjectFactory pgpFact = new PGPObjectFactory(cData.getDataStream());
                message = pgpFact.nextObject();
            }

            if (message instanceof PGPLiteralData)
            {
                PGPLiteralData ld = (PGPLiteralData) message;
                unc = ld.getInputStream();
            }
            else if (message instanceof PGPOnePassSignatureList)
            {
                throw new PGPException("Encrypted message contains a signed message - not literal data");
            }
            else
            {
                throw new PGPException("Message is not a simple encrypted file - type unknown");
            }
        }
        catch (PGPException e)
        {
            log.error("PGPException: " + e.getMessage(), e);
            if (e.getUnderlyingException() != null)
            {
                log.error("UnderLyingException: " + e.getUnderlyingException().getMessage(), e.getUnderlyingException());
            }
        } finally {
            if (keyIn != null) {
                keyIn.close();
            }
            if (fileIn != null) {
                fileIn.close();
            }
        }
        return unc;
    }
}
