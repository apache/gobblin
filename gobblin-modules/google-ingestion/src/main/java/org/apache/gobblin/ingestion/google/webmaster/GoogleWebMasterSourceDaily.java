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

package org.apache.gobblin.ingestion.google.webmaster;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.services.webmasters.WebmastersScopes;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.extract.google.GoogleCommon;
import org.apache.gobblin.source.extractor.extract.google.GoogleCommonKeys;
import org.apache.gobblin.source.extractor.partition.Partition;
import org.apache.gobblin.source.extractor.watermark.DateWatermark;
import org.apache.gobblin.source.extractor.watermark.TimestampWatermark;

import static org.apache.gobblin.configuration.ConfigurationKeys.SOURCE_CONN_PRIVATE_KEY;
import static org.apache.gobblin.configuration.ConfigurationKeys.SOURCE_CONN_USERNAME;
import static org.apache.gobblin.configuration.ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT;
import static org.apache.gobblin.configuration.ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL;


/**
 * The logic of calculating the watermarks in this GoogleWebMasterSourceDaily only works with the configuration below:
 *
 * source.querybased.watermark.type=hour
 * source.querybased.partition.interval=24
 */
@Slf4j
public class GoogleWebMasterSourceDaily extends GoogleWebMasterSource {

  @Override
  GoogleWebmasterExtractor createExtractor(WorkUnitState state, Map<String, Integer> columnPositionMap,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics, JsonArray schemaJson)
      throws IOException {
    Preconditions.checkArgument(
        state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE).compareToIgnoreCase("Hour") == 0);
    Preconditions.checkArgument(state.getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_PARTITION_INTERVAL) == 24);

    Partition partition = Partition.deserialize(state.getWorkunit());
    long lowWatermark = partition.getLowWatermark();
    long expectedHighWatermark = partition.getHighWatermark();

    /*
      This change is needed because
      1. The partition behavior changed due to commit 7d730fcb0263b8ca820af0366818160d638d1336 [7d730fc]
       by zxcware <zxcware@gmail.com> on April 3, 2017 at 11:47:41 AM PDT
      2. Google Search Console API only cares about Dates, and are both side inclusive.
      Therefore, do the following processing.
     */
    int dateDiff = partition.isHighWatermarkInclusive() ? 1 : 0;
    long highWatermarkDate = DateWatermark.adjustWatermark(Long.toString(expectedHighWatermark), dateDiff);
    long updatedExpectedHighWatermark = TimestampWatermark.adjustWatermark(Long.toString(highWatermarkDate), -1);
    updatedExpectedHighWatermark = Math.max(lowWatermark, updatedExpectedHighWatermark);

    GoogleWebmasterClientImpl gscClient =
        new GoogleWebmasterClientImpl(getCredential(state), state.getProp(ConfigurationKeys.SOURCE_ENTITY));
    return new GoogleWebmasterExtractor(gscClient, state, lowWatermark, updatedExpectedHighWatermark, columnPositionMap,
        requestedDimensions, requestedMetrics, schemaJson);
  }

  private static Credential getCredential(State wuState) {
    String scope = wuState.getProp(GoogleCommonKeys.API_SCOPES, WebmastersScopes.WEBMASTERS_READONLY);
    Preconditions.checkArgument(Objects.equals(WebmastersScopes.WEBMASTERS_READONLY, scope) || Objects
            .equals(WebmastersScopes.WEBMASTERS, scope),
        "The scope for WebMaster must either be WEBMASTERS_READONLY or WEBMASTERS");

    String credentialFile = wuState.getProp(SOURCE_CONN_PRIVATE_KEY);
    List<String> scopes = Collections.singletonList(scope);

//    return GoogleCredential.fromStream(new FileInputStream(credentialFile))
//        .createScoped(Collections.singletonList(scope));

    return new GoogleCommon.CredentialBuilder(credentialFile, scopes)
        .fileSystemUri(wuState.getProp(GoogleCommonKeys.PRIVATE_KEY_FILESYSTEM_URI))
        .proxyUrl(wuState.getProp(SOURCE_CONN_USE_PROXY_URL)).port(wuState.getProp(SOURCE_CONN_USE_PROXY_PORT))
        .serviceAccountId(wuState.getProp(SOURCE_CONN_USERNAME)).build();
  }
}
