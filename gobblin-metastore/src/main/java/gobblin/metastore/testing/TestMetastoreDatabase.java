/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metastore.testing;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Properties;

import gobblin.metastore.MetaStoreModule;
import org.apache.http.client.utils.URIBuilder;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.distribution.Version;

import gobblin.configuration.ConfigurationKeys;


public class TestMetastoreDatabase implements Closeable {
    private static final String DATABASE = "gobblin";
    private final MysqldConfig config;
    private final EmbeddedMysql testingMySqlServer;

    public TestMetastoreDatabase() throws Exception {
        config = MysqldConfig.aMysqldConfig(Version.v5_6_latest)
                .withPort(chooseRandomPort())
                .withUser("testUser", "testPassword")
                .build();

        testingMySqlServer = EmbeddedMysql.anEmbeddedMysql(config)
                .addSchema(DATABASE)
                .start();
        Properties properties = new Properties();
        properties.setProperty(ConfigurationKeys.JOB_HISTORY_STORE_URL_KEY, getJdbcUrl());
        prepareDatabase(properties);
    }

    @Override
    public void close() throws IOException {
        if (testingMySqlServer != null) {
            testingMySqlServer.stop();
        }
    }

    public String getJdbcUrl() throws URISyntaxException {
        final String jdbcPrefix = "jdbc:";
        URIBuilder uri = new URIBuilder();
        uri.setScheme("mysql");
        uri.setHost("localhost");
        uri.setPort(config.getPort());
        uri.setPath("/" + DATABASE);
        uri.addParameter("user", config.getUsername());
        uri.addParameter("password", config.getPassword());
        uri.addParameter("useLegacyDatetimeCode", "false");
        uri.addParameter("rewriteBatchedStatements", "true");
        return jdbcPrefix + uri.build().toString();
    }

    private int chooseRandomPort() throws IOException {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            return socket.getLocalPort();
        } finally {
            if (socket != null) {
                socket.close();
            }
        }
    }

    private void prepareDatabase(Properties properties)
            throws Exception {
        // Read the DDL statements
        List<String> statementLines = Lists.newArrayList();
        List<String> lines = Files.readLines(
                new File("gobblin-metastore/src/main/resources/gobblin_job_history_store.sql"),
                ConfigurationKeys.DEFAULT_CHARSET_ENCODING);
        for (String line : lines) {
            // Skip a comment line
            if (line.startsWith("--")) {
                continue;
            }
            statementLines.add(line);
        }
        String statements = Joiner.on("\n").skipNulls().join(statementLines);

        Optional<Connection> connectionOptional = Optional.absent();
        try {
            Injector injector = Guice.createInjector(new MetaStoreModule(properties));
            DataSource dataSource = injector.getInstance(DataSource.class);
            connectionOptional = Optional.of(dataSource.getConnection());
            Connection connection = connectionOptional.get();
            for (String statement : Splitter.on(";").omitEmptyStrings().trimResults().split(statements)) {
                PreparedStatement preparedStatement = connection.prepareStatement(statement);
                preparedStatement.execute();
            }
        } finally {
            if (connectionOptional.isPresent()) {
                connectionOptional.get().close();
            }
        }
    }
}
