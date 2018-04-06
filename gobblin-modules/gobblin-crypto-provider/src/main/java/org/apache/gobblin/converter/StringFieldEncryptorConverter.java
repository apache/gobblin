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
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Splitter;

import org.apache.gobblin.codec.StreamCodec;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.crypto.EncryptionConfigParser;
import org.apache.gobblin.crypto.EncryptionFactory;
import org.apache.gobblin.recordaccess.RecordAccessor;


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
    Map<String, Object> config = EncryptionConfigParser
        .getConfigForBranch(EncryptionConfigParser.EntityType.CONVERTER_ENCRYPT, getClass().getSimpleName(), workUnit);
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
    RecordAccessor accessor = getRecordAccessor(inputRecord);

    for (String field : fieldsToEncrypt) {
      Map<String, Object> stringsToEncrypt = accessor.getMultiGeneric(field);

      for (Map.Entry<String, Object> entry : stringsToEncrypt.entrySet()) {
        try {
          if (entry.getValue() instanceof String) {
            accessor.set(entry.getKey(), encryptString((String) entry.getValue()));
          } else if (entry.getValue() instanceof List) {
            List<String> encryptedVals = new ArrayList<>();

            for (Object val: (List)entry.getValue()) {
              if (!(val instanceof String)) {
                throw new IllegalArgumentException("Unexpected type " + val.getClass().getCanonicalName() +
                    " while encrypting field " + field);
              }

              encryptedVals.add(encryptString((String)val));
            }

            accessor.setStringArray(entry.getKey(), encryptedVals);
          }
        } catch (IOException | IllegalArgumentException | IllegalStateException e) {
          throw new DataConversionException("Error while encrypting field " + field + ": " + e.getMessage(), e);
        }
      }
    }

    return Collections.singleton(inputRecord);
  }

  private String encryptString(String val)
      throws IOException {
    byte[] bytes = val.getBytes(StandardCharsets.UTF_8);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    OutputStream cipherStream = encryptor.encodeOutputStream(outputStream);
    cipherStream.write(bytes);
    cipherStream.flush();
    cipherStream.close();
    return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
  }

  protected List<String> getFieldsToEncrypt() {
    return fieldsToEncrypt;
  }

  protected abstract RecordAccessor getRecordAccessor(DATA record);
}
