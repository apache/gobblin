package org.apache.gobblin.converter;

import java.util.Map;
import org.apache.gobblin.codec.StreamCodec;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.crypto.EncryptionConfigParser;
import org.apache.gobblin.crypto.EncryptionFactory;


/**
 * Specific implementation of {@link EncryptedSerializedRecordToSerializedRecordConverterBase} that uses Gobblin's
 * {@link EncryptionFactory} to build the proper decryption codec based on config.
 */
public class EncryptedSerializedRecordToSerializedRecordConverter extends EncryptedSerializedRecordToSerializedRecordConverterBase {
  @Override
  protected StreamCodec buildDecryptor(WorkUnitState config) {
    Map<String, Object> decryptionConfig =
        EncryptionConfigParser.getConfigForBranch(EncryptionConfigParser.EntityType.CONVERTER_DECRYPT,
            getClass().getSimpleName(), config);
    if (decryptionConfig == null) {
      throw new IllegalStateException("No decryption config specified in job - can't decrypt!");
    }

    return EncryptionFactory.buildStreamCryptoProvider(decryptionConfig);
  }
}