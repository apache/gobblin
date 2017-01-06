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

package gobblin.data.management.copy.converter;

import javax.annotation.Nullable;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.password.PasswordManager;
import gobblin.util.GPGFileDecrypter;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
 * {@link Converter} that decrypts an {@link InputStream}. Uses utilities in {@link GPGFileDecrypter} to do the actual
 * decryption. It also converts the destination file name by removing .gpg extensions.
 */
public class DecryptConverter extends DistcpConverter {

  private static final String DECRYPTION_PASSPHRASE_KEY = "converter.decrypt.passphrase";
  private static final String GPG_EXTENSION = ".gpg";
  private String passphrase;

  @Override
  public Converter<String, String, FileAwareInputStream, FileAwareInputStream> init(WorkUnitState workUnit) {
    Preconditions.checkArgument(workUnit.contains(DECRYPTION_PASSPHRASE_KEY),
        "Passphrase is required while using DecryptConverter. Please specify " + DECRYPTION_PASSPHRASE_KEY);
    this.passphrase = PasswordManager.getInstance(workUnit).readPassword(workUnit.getProp(DECRYPTION_PASSPHRASE_KEY));
    return super.init(workUnit);
  }

  @Override
  public Function<FSDataInputStream, FSDataInputStream> inputStreamTransformation() {
    return new Function<FSDataInputStream, FSDataInputStream>() {
      @Nullable
      @Override
      public FSDataInputStream apply(FSDataInputStream input) {
        try {
          return GPGFileDecrypter.decryptFile(input, DecryptConverter.this.passphrase);
        } catch (IOException exception) {
          throw new RuntimeException(exception);
        }
      }
    };
  }

  @Override
  public List<String> extensionsToRemove() {
    return Lists.newArrayList(GPG_EXTENSION);
  }

}
