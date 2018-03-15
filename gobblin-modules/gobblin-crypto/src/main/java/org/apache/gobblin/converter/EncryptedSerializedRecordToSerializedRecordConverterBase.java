package org.apache.gobblin.converter;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.gobblin.codec.StreamCodec;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.type.RecordWithMetadata;


/**
 * A converter that converts a encrypted {@link org.apache.gobblin.type.SerializedRecordWithMetadata} to
 * a {@link org.apache.gobblin.type.SerializedRecordWithMetadata}. The decryption algorithm used will be
 * appended to the Transfer-Encoding of the new record.
 */
@Slf4j
public abstract class EncryptedSerializedRecordToSerializedRecordConverterBase extends Converter<String, String, RecordWithMetadata<byte[]>, RecordWithMetadata<byte[]>> {
  private StreamCodec decryptor;

  @Override
  public Converter<String, String, RecordWithMetadata<byte[]>, RecordWithMetadata<byte[]>> init(
      WorkUnitState workUnit) {
    super.init(workUnit);
    decryptor = buildDecryptor(workUnit);
    return this;
  }

  /**
   * Build the StreamCodec that will be used to decrypt each byte record. Must be provided by concrete
   * implementations of this class.
   */
  protected abstract StreamCodec buildDecryptor(WorkUnitState config);

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return "";
  }

  @Override
  public Iterable<RecordWithMetadata<byte[]>> convertRecord(String outputSchema, RecordWithMetadata<byte[]> inputRecord,
      WorkUnitState workUnit)
      throws DataConversionException {
    try {
      ByteArrayInputStream inputStream = new ByteArrayInputStream(inputRecord.getRecord());
      byte[] decryptedBytes;
      try (InputStream decryptedInputStream = decryptor.decodeInputStream(inputStream)) {
        decryptedBytes = IOUtils.toByteArray(decryptedInputStream);
      }
      inputRecord.getMetadata().getGlobalMetadata().addTransferEncoding(decryptor.getTag());

      RecordWithMetadata<byte[]> serializedRecord =
          new RecordWithMetadata<byte[]>(decryptedBytes, inputRecord.getMetadata());
      return Collections.singleton(serializedRecord);
    } catch (Exception e) {
      throw new DataConversionException(e);
    }
  }
}

