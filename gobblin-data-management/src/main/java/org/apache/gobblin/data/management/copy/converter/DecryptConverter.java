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

package org.apache.gobblin.data.management.copy.converter;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.codec.StreamCodec;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.crypto.EncryptionConfigParser;
import org.apache.gobblin.crypto.EncryptionFactory;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.password.PasswordManager;


/**
 * {@link Converter} that decrypts an {@link InputStream}.
 *
 * The encryption algorithm will be selected by looking at the {@code converter.encrypt.algorithm} configuration key
 * in the job. See {@link EncryptionConfigParser} for more details.
 *
 * If no algorithm is specified then the converter will default to gpg for backwards compatibility
 * reasons.
 */
@Slf4j
public class DecryptConverter extends DistcpConverter {

  private static final String DEFAULT_ALGORITHM = "gpg"; // for backwards compatibility

  private static final String DECRYPTION_PASSPHRASE_KEY = "converter.decrypt.passphrase";
  private StreamCodec decryptor;

  @Override
  public Converter<String, String, FileAwareInputStream, FileAwareInputStream> init(WorkUnitState workUnit) {
    Map<String, Object> config =
        EncryptionConfigParser.getConfigForBranch(EncryptionConfigParser.EntityType.CONVERTER, workUnit);

   if (config == null) {
     // Backwards compatibility check: if no config was passed in via the standard config, revert back to GPG
     // with the passphrase in DECRYPTION_PASSPHRASE_KEY.
     log.info("Assuming GPG decryption since no other config parameters are set");
     config = Maps.newHashMap();

      config.put(EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, DEFAULT_ALGORITHM);
      Preconditions.checkArgument(workUnit.contains(DECRYPTION_PASSPHRASE_KEY),
          "Passphrase is required while using DecryptConverter. Please specify " + DECRYPTION_PASSPHRASE_KEY);
      String passphrase =
          PasswordManager.getInstance(workUnit).readPassword(workUnit.getProp(DECRYPTION_PASSPHRASE_KEY));
      config.put(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY, passphrase);
    }

    decryptor = EncryptionFactory.buildStreamCryptoProvider(config);
    return super.init(workUnit);
  }

  @Override
  public Function<InputStream, InputStream> inputStreamTransformation() {
    return new Function<InputStream, InputStream>() {
      @Nullable
      @Override
      public InputStream apply(InputStream input) {
        try {
          return decryptor.decodeInputStream(input);
        } catch (IOException exception) {
          throw new RuntimeException(exception);
        }
      }
    };
  }

  @Override
  public List<String> extensionsToRemove() {
    return Lists.newArrayList("." + decryptor.getTag());
  }
}
