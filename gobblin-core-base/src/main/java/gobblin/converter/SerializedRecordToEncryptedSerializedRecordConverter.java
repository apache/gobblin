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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.WorkUnitState;
import gobblin.crypto.EncryptionConfigParser;
import gobblin.crypto.EncryptionFactory;
import gobblin.type.SerializedRecord;
import gobblin.util.io.StreamUtils;
import gobblin.writer.StreamCodec;


/**
 * A converter that converts a {@link SerializedRecord} to a new {@link SerializedRecord}
 * where the serialized bytes represent encrypted data. The encryption algorithm used will be
 * appended to the content type of the new SerializedRecord object.
 */
@Slf4j
public class SerializedRecordToEncryptedSerializedRecordConverter extends Converter<String, String, SerializedRecord, SerializedRecord> {
  private StreamCodec encryptor;

  @Override
  public Converter<String, String, SerializedRecord, SerializedRecord> init(WorkUnitState workUnit) {
    super.init(workUnit);

    Map<String, Object> encryptionConfig = EncryptionConfigParser.getConfigForBranch(workUnit);
    if (encryptionConfig == null) {
      throw new IllegalStateException("No encryption config specified in job - can't encrypt!");
    }

    encryptor = EncryptionFactory.buildStreamEncryptor(encryptionConfig);
    return this;
  }

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return "";
  }

  @Override
  public Iterable<SerializedRecord> convertRecord(String outputSchema, SerializedRecord inputRecord,
      WorkUnitState workUnit)
      throws DataConversionException {
    try {
      ByteArrayOutputStream bOs = new ByteArrayOutputStream();
      OutputStream encryptedStream = encryptor.encodeOutputStream(bOs);
      StreamUtils.byteBufferToOutputStream(inputRecord.getRecord(), encryptedStream);

      List<String> contentTypes =
          ImmutableList.<String>builder().addAll(inputRecord.getContentTypes()).add(encryptor.getTag()).build();

      SerializedRecord serializedRecord = new SerializedRecord(ByteBuffer.wrap(bOs.toByteArray()), contentTypes);

      return new SingleRecordIterable<>(serializedRecord);
    } catch (Exception e) {
      throw new DataConversionException(e);
    }
  }
}
