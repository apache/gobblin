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

package org.apache.gobblin.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import groovy.grape.Grape;
import groovy.lang.GroovyClassLoader;


/**
 * Utility class for downloads using grape
 */
public class DownloadUtils {
  public static final String IVY_SETTINGS_FILE_NAME = "ivysettings.xml";

  /**
   * Download jar through {@link Grape} given an org, module and version
   * It is assumed that an ivy settings file exists on the classpath
   */
  public static URI[] downloadJar(String org, String module, String version, boolean transitive) throws IOException {
    Map<String, Object> artifactMap = Maps.newHashMap();
    artifactMap.put("org", org);
    artifactMap.put("module", module);
    artifactMap.put("version", version);
    artifactMap.put("transitive", transitive);
    return downloadJar(artifactMap);
  }

  public static URI[] downloadJar(Map<String, Object> artifactMap) throws IOException {
    System.setProperty("grape.config", getIvySettingsFile().getAbsolutePath());

    Map<String, Object> args = Maps.newHashMap();
    args.put("classLoader",  AccessController.doPrivileged(new PrivilegedAction<GroovyClassLoader>() {
      @Override
      public GroovyClassLoader run() {
        return new GroovyClassLoader();
      }
    }));
    return Grape.resolve(args, artifactMap);
  }

  /**
   * Get ivy settings file from classpath
   */
  public static File getIvySettingsFile() throws IOException {
    URL settingsUrl = Thread.currentThread().getContextClassLoader().getResource(IVY_SETTINGS_FILE_NAME);
    if (settingsUrl == null) {
      throw new IOException("Failed to find " + IVY_SETTINGS_FILE_NAME + " from class path");
    }

    // Check if settingsUrl is file on classpath
    File ivySettingsFile = new File(settingsUrl.getFile());
    if (ivySettingsFile.exists()) {
      // can access settingsUrl as a file
      return ivySettingsFile;
    }

    // Create temporary Ivy settings file.
    ivySettingsFile = File.createTempFile("ivy.settings", ".xml");
    ivySettingsFile.deleteOnExit();

    try (OutputStream os = new BufferedOutputStream(new FileOutputStream(ivySettingsFile))) {
      Resources.copy(settingsUrl, os);
    }

    return ivySettingsFile;
  }
}
