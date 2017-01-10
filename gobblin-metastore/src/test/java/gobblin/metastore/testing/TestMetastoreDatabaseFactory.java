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

package gobblin.metastore.testing;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;


public class TestMetastoreDatabaseFactory {
    private static final Object syncObject = new Object();
    private static TestMetastoreDatabaseServer testMetastoreDatabaseServer;
    private static Set<ITestMetastoreDatabase> instances = new HashSet<>();

    private TestMetastoreDatabaseFactory() {
    }

    public static Config getDefaultConfig() {
      return ConfigFactory.defaultOverrides().withFallback(ConfigFactory.load());
    }

    public static ITestMetastoreDatabase get() throws Exception {
        return TestMetastoreDatabaseFactory.get("latest");
    }

    public static ITestMetastoreDatabase get(String version) throws Exception {
        return get(version, getDefaultConfig());
    }

    public static ITestMetastoreDatabase get(String version, Config dbConfig) throws Exception {
        try {
            synchronized (syncObject) {
                ensureDatabaseExists(dbConfig);
                TestMetadataDatabase instance = new TestMetadataDatabase(testMetastoreDatabaseServer, version);
                instances.add(instance);
                return instance;
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create TestMetastoreDatabase with version " + version +
               " and config " + dbConfig.root().render(ConfigRenderOptions.defaults().setFormatted(true).setJson(true))
               + " cause: " + e, e);
        }

    }

    static void release(ITestMetastoreDatabase instance) throws IOException {
        synchronized (syncObject) {
            if (instances.remove(instance) && instances.size() == 0) {
                testMetastoreDatabaseServer.close();
                testMetastoreDatabaseServer = null;
            }
        }
    }

    private static void ensureDatabaseExists(Config dbConfig) throws Exception {
        if (testMetastoreDatabaseServer == null) {
            try (Mutex ignored = new Mutex()) {
                if (testMetastoreDatabaseServer == null) {
                    testMetastoreDatabaseServer = new TestMetastoreDatabaseServer(dbConfig);
                }
            }
        }
    }

    private static class Mutex implements Closeable {
        private final Object syncObject = new Object();
        private final AtomicBoolean isLocked = new AtomicBoolean(false);
        private FileChannel fileChannel;
        private FileLock fileLock;

        public Mutex() throws IOException {
            take();
        }

        @Override
        public void close() {
            release();
        }

        private boolean take() throws IOException {
            if (!isLocked.get()) {
                synchronized (syncObject) {
                    if (!isLocked.get()) {
                        if (fileChannel == null) {
                            Path lockPath = Paths.get(System.getProperty("user.home")).resolve(".embedmysql.lock");
                            fileChannel = FileChannel.open(lockPath, StandardOpenOption.CREATE,
                                    StandardOpenOption.WRITE, StandardOpenOption.READ);
                        }
                        fileLock = fileChannel.lock();
                        isLocked.set(true);
                        return true;
                    }
                    return true;
                }
            }
            return true;
        }

        private boolean release() {
            if (isLocked.get()) {
                synchronized (syncObject) {
                    if (isLocked.get()) {
                        if (fileLock != null) {
                            boolean result = true;
                            try {
                                fileLock.close();
                                fileLock = null;
                                isLocked.set(false);
                            } catch (IOException ignored) {
                                result = false;
                            }
                            if (fileChannel != null) {
                                try {
                                    fileChannel.close();
                                } catch (IOException ignored) {
                                    result = false;
                                }
                            }
                            return result;
                        }
                        isLocked.set(false);
                        return true;
                    }
                    return true;
                }
            }
            return true;
        }
    }
}

