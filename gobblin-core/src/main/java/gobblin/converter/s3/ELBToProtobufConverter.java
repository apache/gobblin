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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author ahollenbach@nerdwallet.com
 */
public class ELBToProtobufConverter extends Converter<Class<ELB>, Class<LogFile>, ELB, LogFile> {

  private static final Logger LOG = LoggerFactory.getLogger(ELBToProtobufConverter.class);

  protected static final String ISO8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.";
  protected static final String LOG_DATE_FORMAT = "yyyy-MM-dd";
  protected static final String LOG_TIME_FORMAT = "HH:mm:ss";

  @Override
  public Class<LogFile> convertSchema(Class<ELB> inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return LogFile.class;
  }

  @Override
  public Iterable<LogFile> convertRecord(Class<LogFile> outputSchema, ELB elbRecord, WorkUnitState workUnit) throws DataConversionException {
    LogFile logFile = LogFile.newBuilder()
            .setDate(elbRecord.getDate())
            .setTime(elbRecord.getTime())
            .setName(elbRecord.getElbName())
            .setCIp(elbRecord.getClientIp())
            .setSHost(elbRecord.getBackendIp())
            .setTimeTaken(elbRecord.getTimeTaken())
            .setScStatus(elbRecord.getBackendStatusCode())
            .setScBytes(elbRecord.getSentBytes())
            .setCsMethod(elbRecord.getRequestMethod())
            .setUri(elbRecord.getRequestUri())
            .setUserAgent(elbRecord.getUserAgent())
            .build();

    return new SingleRecordIterable<LogFile>(logFile);
  }


}
