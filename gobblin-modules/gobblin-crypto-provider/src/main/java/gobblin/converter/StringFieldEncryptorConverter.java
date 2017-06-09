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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Splitter;

import gobblin.codec.StreamCodec;
import gobblin.configuration.WorkUnitState;
import gobblin.crypto.EncryptionConfigParser;
import gobblin.crypto.EncryptionFactory;
import gobblin.recordaccess.RecordAccessor;


/**
 * Converter that can encrypt a string field in place. Assumes that the encryption algorithm chosen will output
 * a UTF-8 encoded byte array.
 */
public abstract class StringFieldEncryptorConverter<SCHEMA, DATA> extends Converter<SCHEMA, SCHEMA, DATA, DATA> {
  public static final String FIELDS_TO_ENCRYPT_CONFIG_NAME = "converter.fieldsToEncrypt";

  private StreamCodec encryptor;
  private List<String> fieldsToEncrypt;

  @Override
  public Converter<SCHEMA, SCHEMA, DATA, DATA> init(WorkUnitState workUnit) {
    super.init(workUnit);
    Map<String, Object> config =
        EncryptionConfigParser.getConfigForBranch(EncryptionConfigParser.EntityType.CONVERTER, getClass().getSimpleName(), workUnit);
    encryptor = EncryptionFactory.buildStreamCryptoProvider(config);

    String fieldsToEncryptConfig = workUnit.getProp(FIELDS_TO_ENCRYPT_CONFIG_NAME, null);
    if (fieldsToEncryptConfig == null) {
      throw new IllegalArgumentException("Must fill in the " + FIELDS_TO_ENCRYPT_CONFIG_NAME + " config option!");
    }

    fieldsToEncrypt = Splitter.on(',').splitToList(fieldsToEncryptConfig);

    return this;
  }

  @Override
  public SCHEMA convertSchema(SCHEMA inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<DATA> convertRecord(SCHEMA outputSchema, DATA inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      RecordAccessor accessor = getRecordAccessor(inputRecord);

      for (String field : fieldsToEncrypt) {
        Map<String, String> stringsToEncrypt = accessor.getMultiAsString(field);

        for (Map.Entry<String, String> entry : stringsToEncrypt.entrySet()) {
          byte[] bytes = entry.getValue().getBytes(StandardCharsets.UTF_8);

          ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

          OutputStream cipherStream = encryptor.encodeOutputStream(outputStream);
          cipherStream.write(bytes);
          cipherStream.flush();
          cipherStream.close();

          byte[] cipherBytes = outputStream.toByteArray();
          accessor.set(entry.getKey(), new String(cipherBytes, StandardCharsets.UTF_8));
        }
      }

      return Collections.singleton(inputRecord);
    } catch (IOException e) {
      throw new DataConversionException("Error encrypting field", e);
    }
  }

  protected List<String> getFieldsToEncrypt() {
    return fieldsToEncrypt;
  }

  protected abstract RecordAccessor getRecordAccessor(DATA record);
}
