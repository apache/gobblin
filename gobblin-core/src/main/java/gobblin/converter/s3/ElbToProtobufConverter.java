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
import gobblin.converter.s3.LogFileProtobuf.LogFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * @author ahollenbach@nerdwallet.com
 */
public class ElbToProtobufConverter extends Converter<Class<String>, Class<LogFile>, ArrayList<String>, LogFile> {

  private static final Logger LOG = LoggerFactory.getLogger(ElbToProtobufConverter.class);


  @Override
  public Class<LogFile> convertSchema(Class<String> inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return LogFile.class;
  }

  @Override
  public Iterable<LogFile> convertRecord(Class<LogFile> outputSchema, ArrayList<String> inputRecord, WorkUnitState workUnit) throws DataConversionException {
    // TODO comment this mess
    LogFile logFile = LogFile.newBuilder()
            .setDate(inputRecord.get(0))                        // TODO parse datetime
            .setTime(inputRecord.get(0))                        // TODO parse datetime
            .setName(inputRecord.get(1))
            .setCIp(inputRecord.get(2))
            .setSHost(inputRecord.get(3))
            .setTimeTaken(Double.parseDouble(inputRecord.get(4))
                        + Double.parseDouble(inputRecord.get(5))
                        + Double.parseDouble(inputRecord.get(6)))
            .setScStatus(Integer.parseInt(inputRecord.get(8)))
            .setScBytes(Integer.parseInt(inputRecord.get(10)))  // TODO rename? naming ambiguity with Bytes
            .setUri(inputRecord.get(11))                        // TODO parse out DNS name
            .setUserAgent(inputRecord.get(12))
            .build();

    LOG.info(logFile.getScStatus() + "|" + logFile.getCIp() + "|" + logFile.getTimeTaken());

    return new SingleRecordIterable<LogFile>(logFile);
  }
}
