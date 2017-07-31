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
package org.apache.gobblin.converter.http;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigSyntax;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;

/**
 * Converts Avro to RestJsonEntry.
 * This converter won't provide converted Schema mainly because:
 * 1. The purpose of this converter is to convert DataRecord to fit JSON REST API writer. This converter is
 *    intended to be end of the converter chain and expect to be followed by JSON REST API writer.
 * 2. JSON schema is still under development and there is no widely accepted JSON Schema.
 */
public class AvroToRestJsonEntryConverter extends Converter<Schema, Void, GenericRecord, RestEntry<JsonObject>> {
  //Resource template ( e.g: /sobject/account/${account_id} )
  static final String CONVERTER_AVRO_REST_ENTRY_RESOURCE_KEY = "converter.avro.rest.resource_key";
  //JSON conversion template @see convertRecord
  static final String CONVERTER_AVRO_REST_JSON_ENTRY_TEMPLATE = "converter.avro.rest.json_hocon_template";

  private final JsonParser parser = new JsonParser();

  @Override
  public Void convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return null;
  }

  /**
   * Use resource key(Optional) and rest json entry as a template and fill in template using Avro as a reference.
   * e.g:
   *  Rest JSON entry HOCON template:
   *    AccountId=${sf_account_id},Member_Id__c=${member_id}
   *  Avro:
   *    {"sf_account_id":{"string":"0016000000UiCYHAA3"},"member_id":{"long":296458833}}
   *
   *  Converted Json:
   *    {"AccountId":"0016000000UiCYHAA3","Member_Id__c":296458833}
   *
   *  As it's template based approach, it can produce nested JSON structure even Avro is flat (or vice versa).
   *
   * e.g:
   *  Rest resource template:
   *    /sobject/account/memberId/${member_id}
   *  Avro:
   *    {"sf_account_id":{"string":"0016000000UiCYHAA3"},"member_id":{"long":296458833}}
   *  Converted resource:
   *    /sobject/account/memberId/296458833
   *
   *  Converted resource will be used to form end point.
   *    http://www.server.com:9090/sobject/account/memberId/296458833
   *
   * {@inheritDoc}
   * @see org.apache.gobblin.converter.Converter#convertRecord(java.lang.Object, java.lang.Object, org.apache.gobblin.configuration.WorkUnitState)
   */
  @Override
  public Iterable<RestEntry<JsonObject>> convertRecord(Void outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    Config srcConfig = ConfigFactory.parseString(inputRecord.toString(),
                                                 ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON));

    String resourceKey = workUnit.getProp(CONVERTER_AVRO_REST_ENTRY_RESOURCE_KEY, "");
    if(!StringUtils.isEmpty(resourceKey)) {
      final String dummyKey = "DUMMY";
      Config tmpConfig = ConfigFactory.parseString(dummyKey + "=" + resourceKey).resolveWith(srcConfig);
      resourceKey = tmpConfig.getString(dummyKey);
    }

    String hoconInput = workUnit.getProp(CONVERTER_AVRO_REST_JSON_ENTRY_TEMPLATE);
    if(StringUtils.isEmpty(hoconInput)) {
      return new SingleRecordIterable<>(new RestEntry<>(resourceKey, parser.parse(inputRecord.toString()).getAsJsonObject()));
    }

    Config destConfig = ConfigFactory.parseString(hoconInput).resolveWith(srcConfig);
    JsonObject json = parser.parse(destConfig.root().render(ConfigRenderOptions.concise())).getAsJsonObject();
    return new SingleRecordIterable<>(new RestEntry<>(resourceKey, json));
  }
}