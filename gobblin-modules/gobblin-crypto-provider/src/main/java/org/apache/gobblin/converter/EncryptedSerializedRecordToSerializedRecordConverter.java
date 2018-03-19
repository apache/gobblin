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

