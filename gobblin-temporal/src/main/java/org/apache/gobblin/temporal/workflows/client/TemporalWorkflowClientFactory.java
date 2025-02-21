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

package org.apache.gobblin.temporal.workflows.client;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.ddm.work.assistance.MDCContextPropagator;
import org.apache.gobblin.temporal.workflows.metrics.TemporalMetricsHelper;
import org.apache.gobblin.temporal.workflows.service.ManagedWorkflowServiceStubs;
import org.apache.gobblin.util.ConfigUtils;


public class TemporalWorkflowClientFactory {
    public static ManagedWorkflowServiceStubs createServiceInstance(String connectionUri) throws Exception {
        GobblinClusterUtils.setSystemProperties(ConfigFactory.load());
        Config config = GobblinClusterUtils.addDynamicConfig(ConfigFactory.load());
        String SHARED_KAFKA_CONFIG_PREFIX_WITH_DOT = "gobblin.kafka.sharedConfig.";
        String SSL_KEYMANAGER_ALGORITHM = SHARED_KAFKA_CONFIG_PREFIX_WITH_DOT + "ssl.keymanager.algorithm";
        String SSL_KEYSTORE_TYPE = SHARED_KAFKA_CONFIG_PREFIX_WITH_DOT + "ssl.keystore.type";
        String SSL_KEYSTORE_LOCATION = SHARED_KAFKA_CONFIG_PREFIX_WITH_DOT + "ssl.keystore.location";
        String SSL_KEY_PASSWORD = SHARED_KAFKA_CONFIG_PREFIX_WITH_DOT + "ssl.key.password";
        String SSL_TRUSTSTORE_LOCATION = SHARED_KAFKA_CONFIG_PREFIX_WITH_DOT + "ssl.truststore.location";
        String SSL_TRUSTSTORE_PASSWORD = SHARED_KAFKA_CONFIG_PREFIX_WITH_DOT + "ssl.truststore.password";

        List<String> SSL_CONFIG_DEFAULT_SSL_PROTOCOLS = Collections.unmodifiableList(
                Arrays.asList("TLSv1.2"));
        List<String> SSL_CONFIG_DEFAULT_CIPHER_SUITES = Collections.unmodifiableList(Arrays.asList(
                // The following list is from https://github.com/netty/netty/blob/4.1/codec-http2/src/main/java/io/netty/handler/codec/http2/Http2SecurityUtil.java#L50
                "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",

                /* REQUIRED BY HTTP/2 SPEC */
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                /* REQUIRED BY HTTP/2 SPEC */

                "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
                "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
                "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
                "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"
        ));

        String keyStoreType = config.getString(SSL_KEYSTORE_TYPE);
        File keyStoreFile = new File(config.getString(SSL_KEYSTORE_LOCATION));
        String keyStorePassword = config.getString(SSL_KEY_PASSWORD);

        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        keyStore.load(toInputStream(keyStoreFile), keyStorePassword.toCharArray());

        // Set key manager from key store
        String sslKeyManagerAlgorithm = config.getString(SSL_KEYMANAGER_ALGORITHM);
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(sslKeyManagerAlgorithm);
        keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());

        // Set trust manager from trust store
        KeyStore trustStore = KeyStore.getInstance("JKS");
        File trustStoreFile = new File(config.getString(SSL_TRUSTSTORE_LOCATION));

        String trustStorePassword = config.getString(SSL_TRUSTSTORE_PASSWORD);
        trustStore.load(toInputStream(trustStoreFile), trustStorePassword.toCharArray());
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("SunX509");
        trustManagerFactory.init(trustStore);

        SslContext sslContext = GrpcSslContexts.forClient()
                .keyManager(keyManagerFactory)
                .trustManager(trustManagerFactory)
                .protocols(SSL_CONFIG_DEFAULT_SSL_PROTOCOLS)
                .ciphers(SSL_CONFIG_DEFAULT_CIPHER_SUITES)
                .build();

        // Initialize metrics
        int reportInterval = ConfigUtils.getInt(config, GobblinTemporalConfigurationKeys.TEMPORAL_METRICS_REPORT_INTERVAL_SECS,
            GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_METRICS_REPORT_INTERVAL_SECS);
        Scope metricsScope = new RootScopeBuilder()
            .reporter(TemporalMetricsHelper.getStatsReporter(config))
            .tags(TemporalMetricsHelper.getDimensions(config))
            .reportEvery(com.uber.m3.util.Duration.ofSeconds(reportInterval));

        WorkflowServiceStubsOptions options = WorkflowServiceStubsOptions.newBuilder()
                .setTarget(connectionUri)
                .setEnableHttps(true)
                .setSslContext(sslContext)
                .setMetricsScope(metricsScope)
                .build();

        return new ManagedWorkflowServiceStubs(WorkflowServiceStubs.newServiceStubs(options));
    }

    public static WorkflowClient createClientInstance(WorkflowServiceStubs service, String namespace) {
        WorkflowClientOptions options = WorkflowClientOptions.newBuilder().setNamespace(namespace)
            .setContextPropagators(Collections.singletonList(new MDCContextPropagator()))
            .build();
        return WorkflowClient.newInstance(service, options);
    }

    private static InputStream toInputStream(File storeFile)
        throws IOException {
        byte[] data = FileUtils.readFileToByteArray(storeFile);
        return new ByteArrayInputStream(data);
    }
}
