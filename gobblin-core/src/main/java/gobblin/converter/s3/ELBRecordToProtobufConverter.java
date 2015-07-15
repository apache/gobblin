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
import logFile.ELBLogProto.ELBLog;
import logFile.ServerLogHeaderProto.*;


/**
 * Converts an ELB record to the {@link ELBLogProto} serialized protobuf format.
 *
 * @author ahollenbach@nerdwallet.com
 */
public class ELBRecordToProtobufConverter extends Converter<Class<ELBRecord>, Class<ELBLog>, ELBRecord, ELBLog> {

  @Override
  public Class<ELBLog> convertSchema(Class<ELBRecord> inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return ELBLog.class;
  }

  @Override
  public Iterable<ELBLog> convertRecord(Class<ELBLog> outputSchema, ELBRecord elbRecord, WorkUnitState workUnit)
      throws DataConversionException {
    ServerLogHeader.Builder logHeaderBuilder = ServerLogHeader.newBuilder()
        .setSource(logFile.ServerLogHeaderProto.Source.ELB)
        .setTimestamp(elbRecord.getTimestampInMillis())
        .setTimeTaken(elbRecord.getTimeTaken())
        .setServerHost(elbRecord.getBackendIp())
        .setClientToServerUriStem(elbRecord.getRequestPath())
        .setServerToClientStatus(elbRecord.getBackendStatusCode())
        .setServerToClientBytes(elbRecord.getSentBytes())
        .setServerPort(elbRecord.getBackendPort())
        .setClientToServerMethod(elbRecord.getRequestMethod())
        .setClientIp(elbRecord.getClientIp())
        .setClientToServerHostHeader(elbRecord.getRequestHostHeader())
        .setClientToServerProtocol(elbRecord.getRequestProtocol())
        .setClientToServerUriFull(elbRecord.getRequestUri())
        // optional fields
        .setClientToServerBytes(elbRecord.getReceivedBytes());

    if (elbRecord.getClientPort() != 0) {
      logHeaderBuilder.setClientPort(elbRecord.getClientPort());
    }
    if (elbRecord.getUserAgent() != null) {
      logHeaderBuilder.setClientToServerUserAgent(elbRecord.getUserAgent());
    }
    if (elbRecord.getRequestHttpVersion() != null) {
      logHeaderBuilder.setClientToServerProtocolVersion(elbRecord.getRequestHttpVersion());
    }
    ServerLogHeader logHeader = logHeaderBuilder.build();


    ELBLog.Builder elbLogBuilder = ELBLog.newBuilder()
        .setHeader(logHeader)
        .setElbName(elbRecord.getElbName())
        .setElbStatusCode(elbRecord.getElbStatusCode());
        // optional fields

    if (elbRecord.getSslCipher() != null) {
      elbLogBuilder.setClientToServerSslCipher(elbRecord.getSslCipher());
    }
    if (elbRecord.getSslProtocol() != null) {
      elbLogBuilder.setClientToServerSslProtocol(elbRecord.getSslProtocol());
    }
    if (elbRecord.getRequestProcessingTime() != -1) {
      elbLogBuilder.setRequestProcessingTime(elbRecord.getRequestProcessingTime());
    }
    if (elbRecord.getBackendProcessingTime() != -1) {
      elbLogBuilder.setBackendProcessingTime(elbRecord.getBackendProcessingTime());
    }
    if (elbRecord.getResponseProcessingTime() != -1) {
      elbLogBuilder.setResponseProcessingTime(elbRecord.getResponseProcessingTime());
    }

    ELBLog log = elbLogBuilder.build();

    return new SingleRecordIterable<ELBLog>(log);
  }
}
