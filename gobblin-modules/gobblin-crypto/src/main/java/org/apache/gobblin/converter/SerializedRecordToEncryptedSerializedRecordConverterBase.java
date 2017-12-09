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

package org.apache.gobblin.converter;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Collections;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.codec.StreamCodec;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.type.RecordWithMetadata;


/**
 * A converter that converts a {@link org.apache.gobblin.type.SerializedRecordWithMetadata} to a {@link org.apache.gobblin.type.SerializedRecordWithMetadata}
 * where the serialized bytes represent encrypted data. The encryption algorithm used will be appended to the
 * Transfer-Encoding of the new record.
 */
@Slf4j
public abstract class SerializedRecordToEncryptedSerializedRecordConverterBase extends Converter<String, String, RecordWithMetadata<byte[]>, RecordWithMetadata<byte[]>> {
  private StreamCodec encryptor;

  @Override
  public Converter<String, String, RecordWithMetadata<byte[]>, RecordWithMetadata<byte[]>> init(
      WorkUnitState workUnit) {
    super.init(workUnit);
    encryptor = buildEncryptor(workUnit);
    return this;
  }

  /**
   * Build the StreamCodec that will be used to encrypt each byte record. Must be provided by concrete
   * implementations of this class.
   */
  protected abstract StreamCodec buildEncryptor(WorkUnitState config);

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
      ByteArrayOutputStream bOs = new ByteArrayOutputStream();
      try (OutputStream encryptedStream = encryptor.encodeOutputStream(bOs)) {
        encryptedStream.write(inputRecord.getRecord());
      }
      inputRecord.getMetadata().getGlobalMetadata().addTransferEncoding(encryptor.getTag());

      RecordWithMetadata<byte[]> serializedRecord =
          new RecordWithMetadata<byte[]>(bOs.toByteArray(), inputRecord.getMetadata());

      return Collections.singleton(serializedRecord);
    } catch (Exception e) {
      throw new DataConversionException(e);
    }
  }
}
