/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy.converter;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.util.GPGFileDecrypter;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchProviderException;

import com.google.common.base.Preconditions;


/**
 * {@link Converter} that decrypts an {@link InputStream}. Uses utilities in {@link GPGFileDecrypter} to do the actual
 * decryption.
 */
public class DecryptConverter extends Converter<String, String, FileAwareInputStream, FileAwareInputStream> {

  private static final String DECRYPTION_PASSPHRASE_KEY = "converter.decrypt.passphrase";
  private String passphrase;

  @Override
  public Converter<String, String, FileAwareInputStream, FileAwareInputStream> init(WorkUnitState workUnit) {
    Preconditions.checkArgument(workUnit.contains(DECRYPTION_PASSPHRASE_KEY),
        "Passphrase is required while using DecryptConverter. Please specify " + DECRYPTION_PASSPHRASE_KEY);
    this.passphrase = workUnit.getProp(DECRYPTION_PASSPHRASE_KEY);
    return super.init(workUnit);
  }

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<FileAwareInputStream> convertRecord(String outputSchema, FileAwareInputStream fileAwareInputStream,
      WorkUnitState workUnit) throws DataConversionException {

    try {
      fileAwareInputStream.setInputStream(GPGFileDecrypter.decryptFile(fileAwareInputStream.getInputStream(),
          passphrase));
      return new SingleRecordIterable<FileAwareInputStream>(fileAwareInputStream);
    } catch (IOException e) {
      throw new DataConversionException(e);
    } catch (NoSuchProviderException e) {
      throw new DataConversionException(e);
    }
  }

}
