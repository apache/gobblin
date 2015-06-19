/* (c) 2015 NerdWallet All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.converter.s3;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.converter.s3.LogFileOuterClass.LogFile;


/**
 * Writes the Protobuf LogFile to a byte array to be read in by a byte writer.
 *
 * @author ahollenbach@nerdwallet.com
 */
public class ProtobufSerializerConverter extends Converter<Class<LogFile>, Class<byte[]>, LogFile, byte[]> {
  @Override
  public Class<byte[]> convertSchema(Class<LogFile> inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return byte[].class;
  }

  @Override
  public Iterable<byte[]> convertRecord(Class<byte[]> outputSchema, LogFile inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return new SingleRecordIterable<byte[]>(inputRecord.toByteArray());
  }
}
