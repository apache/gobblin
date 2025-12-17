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

package org.apache.gobblin.yarn;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;

import org.apache.gobblin.util.ConfigUtils;


/**
 * Resolves the jar cache directory path by validating filesystem state and applying fallback logic.
 *
 * <p>This class separates the concern of computing the resolved jar cache directory from config initialization,
 * making it easier to debug and test. The resolution happens lazily when {@link #resolveJarCachePath()} is called.</p>
 *
 * <p>Resolution logic:</p>
 * <ol>
 *   <li>If JAR_CACHE_DIR is explicitly configured, uses it as-is (for backward compatibility)</li>
 *   <li>Otherwise, validates JAR_CACHE_ROOT_DIR exists on filesystem</li>
 *   <li>If not found, tries FALLBACK_JAR_CACHE_ROOT_DIR</li>
 *   <li>Combines validated root with JAR_CACHE_SUFFIX to form final path</li>
 *   <li>If no valid root found, throws IOException</li>
 * </ol>
 */
public class JarCachePathResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(JarCachePathResolver.class);

  private final Config config;
  private final FileSystem fs;

  public JarCachePathResolver(Config config, FileSystem fs) {
    this.config = config;
    this.fs = fs;
  }

  /**
   * Resolves the jar cache directory path, applying validation and fallback logic.
   *
   * @return the resolved jar cache directory path
   * @throws IOException if filesystem operations fail or no valid root directory is found
   */
  public Path resolveJarCachePath() throws IOException {
    // If JAR_CACHE_DIR is explicitly set, use it as-is (backward compatibility)
    if (config.hasPath(GobblinYarnConfigurationKeys.JAR_CACHE_DIR)) {
      String explicitCacheDir = config.getString(GobblinYarnConfigurationKeys.JAR_CACHE_DIR);
      LOGGER.info("Using explicitly configured JAR_CACHE_DIR: {}", explicitCacheDir);
      return new Path(explicitCacheDir);
    }

    String suffix = ConfigUtils.getString(config, GobblinYarnConfigurationKeys.JAR_CACHE_SUFFIX, "");

    // Try primary root directory
    if (config.hasPath(GobblinYarnConfigurationKeys.JAR_CACHE_ROOT_DIR)) {
      String rootDir = config.getString(GobblinYarnConfigurationKeys.JAR_CACHE_ROOT_DIR);
      Path resolvedPath = validateAndComputePath(rootDir, suffix, GobblinYarnConfigurationKeys.JAR_CACHE_ROOT_DIR);
      if (resolvedPath != null) {
        return resolvedPath;
      }
    }

    // Try fallback root directory
    if (config.hasPath(GobblinYarnConfigurationKeys.FALLBACK_JAR_CACHE_ROOT_DIR)) {
      String fallbackRootDir = config.getString(GobblinYarnConfigurationKeys.FALLBACK_JAR_CACHE_ROOT_DIR);
      Path resolvedPath = validateAndComputePath(fallbackRootDir, suffix, GobblinYarnConfigurationKeys.FALLBACK_JAR_CACHE_ROOT_DIR);
      if (resolvedPath != null) {
        return resolvedPath;
      }
    }

    // No valid root directory found - fail fast
    throw new IOException("No valid jar cache root directory found. Please configure " 
        + GobblinYarnConfigurationKeys.JAR_CACHE_ROOT_DIR + " or " 
        + GobblinYarnConfigurationKeys.FALLBACK_JAR_CACHE_ROOT_DIR 
        + " with a valid directory path, or explicitly set " 
        + GobblinYarnConfigurationKeys.JAR_CACHE_DIR);
  }

  /**
   * Validates if the root directory exists and computes the full path with suffix.
   *
   * @param rootDir the root directory to validate
   * @param suffix the suffix to append
   * @param configName the config name for logging
   * @return the computed path if valid, null otherwise
   * @throws IOException if filesystem operations fail
   */
  @VisibleForTesting
  Path validateAndComputePath(String rootDir, String suffix, String configName) throws IOException {
    Path rootPath = new Path(rootDir);
    if (fs.exists(rootPath)) {
      // Strip leading '/' from suffix to ensure proper concatenation
      // Otherwise, Hadoop Path treats it as absolute path and ignores the parent
      String normalizedSuffix = suffix.startsWith("/") ? suffix.substring(1) : suffix;
      Path fullPath = new Path(rootPath, normalizedSuffix);
      LOGGER.info("{} exists: {}, resolved JAR_CACHE_DIR to: {}", configName, rootDir, fullPath);
      return fullPath;
    }
    LOGGER.warn("Configured {} does not exist: {}", configName, rootDir);
    return null;
  }

}

