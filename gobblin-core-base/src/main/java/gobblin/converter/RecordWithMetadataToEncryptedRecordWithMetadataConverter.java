/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import gobblin.configuration.WorkUnitState;
import gobblin.crypto.EncryptionConfigParser;
import gobblin.crypto.EncryptionFactory;
import gobblin.type.RecordWithMetadata;
import gobblin.type.SerializedRecordWithMetadata;
import gobblin.writer.StreamCodec;


/**
 * A converter that converts a {@link RecordWithMetadata} to a {@link gobblin.type.SerializedRecordWithMetadata}
 * where the serialized bytes represent encrypted data.
 */
public class RecordWithMetadataToEncryptedRecordWithMetadataConverter extends Converter<String, String, RecordWithMetadata, SerializedRecordWithMetadata> {
  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return "";
  }

  @Override
  public Iterable<SerializedRecordWithMetadata> convertRecord(String outputSchema, RecordWithMetadata inputRecord,
      WorkUnitState workUnit) throws DataConversionException {

    try {
      if (!(inputRecord.getRecord() instanceof byte[])) {
        throw new DataConversionException(
            "RecordWithMetadataToEncryptedRecordWithMetadataConverter requires input record to be of type byte[]");
      }

      Map<String, Object> encryptionConfig = EncryptionConfigParser.getConfigForBranch(workUnit);
      if (encryptionConfig == null) {
        throw new DataConversionException("No encryption config specified in job - can't encrypt!");
      }

      StreamCodec encryptor = EncryptionFactory.buildStreamEncryptor(encryptionConfig);
      ByteArrayOutputStream bOs = new ByteArrayOutputStream();
      OutputStream encryptedStream = encryptor.encodeOutputStream(bOs);
      encryptedStream.write((byte[])inputRecord.getRecord());
      encryptedStream.close();

      @SuppressWarnings("unchecked")
      SerializedRecordWithMetadata serializedRecordWithMetadata = new SerializedRecordWithMetadata(bOs.toByteArray(),
          inputRecord.getMetadata());
      return new SingleRecordIterable<>(serializedRecordWithMetadata);
    } catch (IOException e) {
      throw new DataConversionException(e);
    }
  }
}
