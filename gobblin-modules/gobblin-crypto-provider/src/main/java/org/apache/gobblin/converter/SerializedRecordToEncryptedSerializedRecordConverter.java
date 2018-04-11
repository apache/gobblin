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

import java.util.Map;

import org.apache.gobblin.codec.StreamCodec;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.crypto.EncryptionConfigParser;
import org.apache.gobblin.crypto.EncryptionFactory;


/**
 * Specific implementation of {@link SerializedRecordToEncryptedSerializedRecordConverterBase} that uses Gobblin's
 * {@link EncryptionFactory} to build the proper encryption codec based on config.
 */
public class SerializedRecordToEncryptedSerializedRecordConverter extends SerializedRecordToEncryptedSerializedRecordConverterBase {
  @Override
  protected StreamCodec buildEncryptor(WorkUnitState config) {
    Map<String, Object> encryptionConfig =
        EncryptionConfigParser.getConfigForBranch(EncryptionConfigParser.EntityType.CONVERTER_ENCRYPT, getClass().getSimpleName(), config);
    if (encryptionConfig == null) {
      throw new IllegalStateException("No encryption config specified in job - can't encrypt!");
    }

    return EncryptionFactory.buildStreamCryptoProvider(encryptionConfig);
  }
}
