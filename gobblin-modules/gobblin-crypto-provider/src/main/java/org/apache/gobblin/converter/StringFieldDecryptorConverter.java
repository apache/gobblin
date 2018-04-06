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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.gobblin.codec.StreamCodec;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.crypto.EncryptionConfigParser;
import org.apache.gobblin.crypto.EncryptionFactory;
import org.apache.gobblin.recordaccess.RecordAccessor;

import com.google.common.base.Splitter;


/**
 * Converter that can decrypt a string field in place. (Note: that means the incoming
 * record will be mutated!). Assumes that the input field is of string
 * type and that the decryption algorithm chosen will output a UTF-8 encoded byte array.
 */
public abstract class StringFieldDecryptorConverter<SCHEMA, DATA> extends Converter<SCHEMA, SCHEMA, DATA, DATA> {
  public static final String FIELDS_TO_DECRYPT_CONFIG_NAME = "converter.fieldsToDecrypt";

  private StreamCodec decryptor;
  private List<String> fieldsToDecrypt;

  @Override
  public Converter<SCHEMA, SCHEMA, DATA, DATA> init(WorkUnitState workUnit) {
    super.init(workUnit);
    Map<String, Object> config = EncryptionConfigParser
        .getConfigForBranch(EncryptionConfigParser.EntityType.CONVERTER_DECRYPT, getClass().getSimpleName(), workUnit);
    decryptor = EncryptionFactory.buildStreamCryptoProvider(config);

    String fieldsToDecryptConfig = workUnit.getProp(FIELDS_TO_DECRYPT_CONFIG_NAME, null);
    if (fieldsToDecryptConfig == null) {
      throw new IllegalArgumentException("Must fill in the " + FIELDS_TO_DECRYPT_CONFIG_NAME + " config option!");
    }

    fieldsToDecrypt = Splitter.on(',').splitToList(fieldsToDecryptConfig);

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

    for (String field : fieldsToDecrypt) {
      Map<String, Object> stringsToDecrypt = accessor.getMultiGeneric(field);
      try {
        for (Map.Entry<String, Object> entry : stringsToDecrypt.entrySet()) {
          if (entry.getValue() instanceof String) {
            String s = decryptString((String) entry.getValue());
            accessor.set(entry.getKey(), s);
          } else if (entry.getValue() instanceof List) {
            List<String> decryptedValues = new ArrayList<>();
            for (Object val : (List)entry.getValue()) {
              if (!(val instanceof String)) {
                throw new IllegalArgumentException("Expected List of Strings, but encountered a value of type "
                  + val.getClass().getCanonicalName());
              }

              decryptedValues.add(decryptString((String)val));
            }

            accessor.setStringArray(entry.getKey(), decryptedValues);
          } else {
            throw new IllegalArgumentException(
                "Expected field to be of type String or List<String>, was " + entry.getValue().getClass()
                    .getCanonicalName());
          }
        }
      } catch (IOException | IllegalArgumentException | IllegalStateException e) {
        throw new DataConversionException("Error while encrypting field " + field + ": " + e.getMessage(), e);
      }
    }

    return Collections.singleton(inputRecord);
  }

  protected List<String> getFieldsToDecrypt() {
    return fieldsToDecrypt;
  }

  protected String decryptString(String val)
      throws IOException {
    byte[] encryptedBytes = val.getBytes(StandardCharsets.UTF_8);

    ByteArrayInputStream inStream = new ByteArrayInputStream(encryptedBytes);

    try (InputStream cipherStream = decryptor.decodeInputStream(inStream);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      IOUtils.copy(cipherStream, outputStream);

      byte[] decryptedBytes = outputStream.toByteArray();
      return new String(decryptedBytes, StandardCharsets.UTF_8);
    }
  }

  protected abstract RecordAccessor getRecordAccessor(DATA record);
}
