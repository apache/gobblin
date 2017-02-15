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

package gobblin.ingestion.google.webmaster;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.services.webmasters.WebmastersScopes;
import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.extractor.extract.google.GoogleCommon;
import gobblin.source.extractor.extract.google.GoogleCommonKeys;

import static gobblin.configuration.ConfigurationKeys.SOURCE_CONN_PRIVATE_KEY;
import static gobblin.configuration.ConfigurationKeys.SOURCE_CONN_USERNAME;
import static gobblin.configuration.ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT;
import static gobblin.configuration.ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL;


public class GoogleWebMasterSourceDaily extends GoogleWebMasterSource {

  @Override
  GoogleWebmasterExtractor createExtractor(WorkUnitState state, Map<String, Integer> columnPositionMap,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics)
      throws IOException {

    long lowWatermark = state.getWorkunit().getLowWatermark(LongWatermark.class).getValue();
    long expectedHighWatermark = state.getWorkunit().getExpectedHighWatermark(LongWatermark.class).getValue();
    GoogleWebmasterClientImpl gscClient =
        new GoogleWebmasterClientImpl(getCredential(state), state.getProp(ConfigurationKeys.SOURCE_ENTITY));

    return new GoogleWebmasterExtractor(gscClient, state, lowWatermark, expectedHighWatermark, columnPositionMap,
        requestedDimensions, requestedMetrics);
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
