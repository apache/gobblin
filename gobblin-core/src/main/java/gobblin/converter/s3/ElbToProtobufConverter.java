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

import gobblin.configuration.ConfigurationKeys;
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
public class ElbToProtobufConverter extends Converter<Class<String>, Class<LogFile>, ArrayList<String>, LogFile> {

  private static final Logger LOG = LoggerFactory.getLogger(ElbToProtobufConverter.class);

  protected static final String ISO8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.";
  protected static final String LOG_DATE_FORMAT = "yyyy-MM-dd";
  protected static final String LOG_TIME_FORMAT = "HH:mm:ss";

  private static final String DEV_STR = "dev";
  private static final String STAGE_STR = "stage";
  private static final String PROD_STR = "prod";

  /**
   * Maps strings (fed in through job file) to Protobuf Environment enum, which then maps to ints
   */
  public static final Map<String, LogFileProtobuf.Environment> environmentMap
          = new HashMap<String, LogFileProtobuf.Environment>() {
            {
              put(DEV_STR,LogFileProtobuf.Environment.DEV);
              put(STAGE_STR,LogFileProtobuf.Environment.STAGE);
              put(PROD_STR,LogFileProtobuf.Environment.PROD);
            }
  };

  @Override
  public Class<LogFile> convertSchema(Class<String> inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return LogFile.class;
  }

  @Override
  public Iterable<LogFile> convertRecord(Class<LogFile> outputSchema, ArrayList<String> inputRecord, WorkUnitState workUnit) throws DataConversionException {
    String envStr = workUnit.getProp(ConfigurationKeys.S3_ENVIRONMENT);
    if (!environmentMap.containsKey(workUnit.getProp(ConfigurationKeys.S3_ENVIRONMENT))) {
      LOG.warn("aws.s3.environment variable not set in job file.");

      envStr = workUnit.getProp(ConfigurationKeys.S3_DEFAULT_ENVIRONMENT);
      LOG.info("Using default: " + envStr);
    }


    Date datetime = parseDate(inputRecord.get(0), new SimpleDateFormat(ISO8601_DATE_FORMAT));
    if (datetime == null) {
      throw new DataConversionException("Failed to parse date. Use the format: " + ISO8601_DATE_FORMAT);
    }

    Request request = new ElbRequest(inputRecord.get(11));

    // TODO comment this mess
    LogFile logFile = LogFile.newBuilder()
            .setEnvironment(environmentMap.get(envStr))
            .setDate(new SimpleDateFormat(LOG_DATE_FORMAT).format(datetime))
            .setTime(new SimpleDateFormat(LOG_TIME_FORMAT).format(datetime))
                    .setName(inputRecord.get(1))
                    .setCIp(inputRecord.get(2))
                    .setSHost(inputRecord.get(3))
                    .setTimeTaken(Double.parseDouble(inputRecord.get(4))
                            + Double.parseDouble(inputRecord.get(5))
                            + Double.parseDouble(inputRecord.get(6)))
                    .setScStatus(Integer.parseInt(inputRecord.get(8)))
                    .setScBytes(Integer.parseInt(inputRecord.get(10)))  // TODO rename? naming ambiguity with Bytes
                    .setCsMethod(request.method)
                    .setUri(request.hostHeader + "/" + request.path)
                    .setUserAgent(inputRecord.get(12))
                    .build();

    return new SingleRecordIterable<LogFile>(logFile);
  }

  public static Date parseDate(String input, SimpleDateFormat dateFormat) {
    // Clean the input
    input = input.trim();

    Date datetime;
    try {
      datetime = dateFormat.parse(input);
    } catch (ParseException e) {
      LOG.error("Failed to parse date:" + input);
      e.printStackTrace();
      return null;
    } catch (ArrayIndexOutOfBoundsException e) {
      LOG.error("Failed to parse date:" + input);
      e.printStackTrace();
      return null;
    } catch (NumberFormatException e) {
      LOG.error("Failed to parse date:" + input);
      e.printStackTrace();
      return null;
    }
    LOG.info("          parse date:" + input);

    return datetime;
  }
}
