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

package org.apache.gobblin.converter.grok;

import java.io.InputStreamReader;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gobblin.configuration.WorkUnitState;

import org.apache.gobblin.converter.DataConversionException;


@Test(groups = {"gobblin.converter"})
public class GrokToJsonConverterTest {
  @Test
  public void convertOutputWithNullableFields()
      throws Exception {
    JsonParser parser = new JsonParser();

    String inputRecord =
        "10.121.123.104 - - [01/Nov/2012:21:01:17 +0100] \"GET /cpc/auth.do?loginsetup=true&targetPage=%2Fcpc%2F HTTP/1.1\" 302 466";

    JsonElement jsonElement = parser
        .parse(new InputStreamReader(getClass().getResourceAsStream("/converter/grok/schemaWithNullableFields.json")));
    JsonArray outputSchema = jsonElement.getAsJsonArray();

    GrokToJsonConverter grokToJsonConverter = new GrokToJsonConverter();
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(GrokToJsonConverter.GROK_PATTERN,
        "^%{IPORHOST:clientip} (?:-|%{USER:ident}) (?:-|%{USER:auth}) \\[%{HTTPDATE:timestamp}\\] \\\"(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|-)\\\" %{NUMBER:response} (?:-|%{NUMBER:bytes})");

    grokToJsonConverter.init(workUnitState);
    JsonObject actual = grokToJsonConverter.convertRecord(outputSchema, inputRecord, workUnitState).iterator().next();

    JsonObject expected =
        parser.parse(new InputStreamReader(getClass().getResourceAsStream("/converter/grok/convertedRecord.json")))
            .getAsJsonObject();
    Assert.assertEquals(actual, expected);
    grokToJsonConverter.close();
  }

  @Test(expectedExceptions = DataConversionException.class)
  public void convertOutputWithNonNullableFieldsShouldThrowDataConversionException()
      throws Exception {
    JsonParser parser = new JsonParser();

    String inputRecord =
        "10.121.123.104 - - [01/Nov/2012:21:01:17 +0100] \"GET /cpc/auth.do?loginsetup=true&targetPage=%2Fcpc%2F HTTP/1.1\" 302 466";

    JsonElement jsonElement = parser.parse(
        new InputStreamReader(getClass().getResourceAsStream("/converter/grok/schemaWithNonNullableFields.json")));
    JsonArray outputSchema = jsonElement.getAsJsonArray();

    GrokToJsonConverter grokToJsonConverter = new GrokToJsonConverter();
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(GrokToJsonConverter.GROK_PATTERN,
        "^%{IPORHOST:clientip} (?:-|%{USER:ident}) (?:-|%{USER:auth}) \\[%{HTTPDATE:timestamp}\\] \\\"(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|-)\\\" %{NUMBER:response} (?:-|%{NUMBER:bytes})");

    grokToJsonConverter.init(workUnitState);
    JsonObject actual = grokToJsonConverter.convertRecord(outputSchema, inputRecord, workUnitState).iterator().next();

    JsonObject expected =
        parser.parse(new InputStreamReader(getClass().getResourceAsStream("/converter/grok/convertedRecord.json")))
            .getAsJsonObject();
    grokToJsonConverter.close();
  }

  @Test
  public void convertWithNullStringSet()
      throws Exception {
    JsonParser parser = new JsonParser();

    String inputRecord =
        "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be mybucket [06/Feb/2014:00:00:38 +0000] 192.0.2.3 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 3E57427F3EXAMPLE REST.GET.VERSIONING - \"GET /mybucket?versioning HTTP/1.1\" 200 - 113 - 7 - \"-\" \"S3Console/0.4\" -";

    JsonElement jsonElement =
        parser.parse(new InputStreamReader(getClass().getResourceAsStream("/converter/grok/s3AccessLogSchema.json")));
    JsonArray outputSchema = jsonElement.getAsJsonArray();

    GrokToJsonConverter grokToJsonConverter = new GrokToJsonConverter();
    WorkUnitState workUnitState = new WorkUnitState();
    //Grok expression was taken from https://github.com/logstash-plugins/logstash-patterns-core/blob/master/patterns/aws
    workUnitState.setProp(GrokToJsonConverter.GROK_PATTERN,
        "%{WORD:owner} %{NOTSPACE:bucket} \\[%{HTTPDATE:timestamp}\\] %{IP:clientip} %{NOTSPACE:requester} %{NOTSPACE:request_id} %{NOTSPACE:operation} %{NOTSPACE:key} (?:\"(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|%{DATA:rawrequest})\"|-) (?:%{INT:response:int}|-) (?:-|%{NOTSPACE:error_code}) (?:%{INT:bytes:int}|-) (?:%{INT:object_size:int}|-) (?:%{INT:request_time_ms:int}|-) (?:%{INT:turnaround_time_ms:int}|-) (?:%{QS:referrer}|-) (?:\"?%{QS:agent}\"?|-) (?:-|%{NOTSPACE:version_id})");
    workUnitState.setProp(GrokToJsonConverter.NULLSTRING_REGEXES, "[\\s-]");

    grokToJsonConverter.init(workUnitState);
    JsonObject actual = grokToJsonConverter.convertRecord(outputSchema, inputRecord, workUnitState).iterator().next();

    JsonObject expected = parser
        .parse(new InputStreamReader(getClass().getResourceAsStream("/converter/grok/convertedS3AccessLogRecord.json")))
        .getAsJsonObject();
    Assert.assertEquals(actual, expected);
    grokToJsonConverter.close();
  }
}