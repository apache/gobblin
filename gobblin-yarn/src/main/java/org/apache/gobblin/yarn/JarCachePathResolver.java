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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;

import org.apache.gobblin.util.ConfigUtils;


/**
 * Utility class for resolving the jar cache directory path by validating filesystem on path existence and applying fallback logic.
 *
 * <p>Resolution logic:</p>
 * <ol>
 *   <li>If JAR_CACHE_DIR is explicitly configured, uses it as-is (for backward compatibility)</li>
 *   <li>Otherwise, validates JAR_CACHE_ROOT_DIR exists on filesystem</li>
 *   <li>If not found, tries FALLBACK_JAR_CACHE_ROOT_DIR</li>
 *   <li>Combines validated root with JAR_CACHE_SUFFIX (or default suffix) to form final path</li>
 *   <li>If no valid root found, throws IOException</li>
 * </ol>
 */
public class JarCachePathResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(JarCachePathResolver.class);
  // Note: Trailing slash will be normalized away by Hadoop Path
  private static final String DEFAULT_JAR_CACHE_SUFFIX = ".gobblinCache/gobblin-temporal";

  // Private constructor to prevent instantiation
  private JarCachePathResolver() {
  }

  /**
   * Resolves the jar cache directory path, applying validation and fallback logic.
   *
   * @param config the configuration
   * @param fs the filesystem to use for validation
   * @return the resolved jar cache directory path
   * @throws IOException if filesystem operations fail or no valid root directory is found
   */
  public static Path resolveJarCachePath(Config config, FileSystem fs) throws IOException {
    // If JAR_CACHE_DIR is explicitly set, use it as-is
    if (config.hasPath(GobblinYarnConfigurationKeys.JAR_CACHE_DIR)) {
      String explicitCacheDir = config.getString(GobblinYarnConfigurationKeys.JAR_CACHE_DIR);
      LOGGER.info("Using explicitly configured JAR_CACHE_DIR: {}", explicitCacheDir);
      return new Path(explicitCacheDir);
    }

    // Get suffix from config, or use default if not configured or empty
    String suffix = ConfigUtils.getString(config, GobblinYarnConfigurationKeys.JAR_CACHE_SUFFIX, "");
    if (suffix == null || suffix.trim().isEmpty()) {
      LOGGER.info("JAR_CACHE_SUFFIX not configured or empty, using default: {}", DEFAULT_JAR_CACHE_SUFFIX);
      suffix = DEFAULT_JAR_CACHE_SUFFIX;
    }

    // Try primary root directory
    if (config.hasPath(GobblinYarnConfigurationKeys.JAR_CACHE_ROOT_DIR)) {
      String rootDir = config.getString(GobblinYarnConfigurationKeys.JAR_CACHE_ROOT_DIR);
      Path resolvedPath = validateAndComputePath(fs, rootDir, suffix, GobblinYarnConfigurationKeys.JAR_CACHE_ROOT_DIR);
      if (resolvedPath != null) {
        return resolvedPath;
      }
    }

    // Try fallback root directory
    if (config.hasPath(GobblinYarnConfigurationKeys.FALLBACK_JAR_CACHE_ROOT_DIR)) {
      String fallbackRootDir = config.getString(GobblinYarnConfigurationKeys.FALLBACK_JAR_CACHE_ROOT_DIR);
      Path resolvedPath = validateAndComputePath(fs, fallbackRootDir, suffix, GobblinYarnConfigurationKeys.FALLBACK_JAR_CACHE_ROOT_DIR);
      if (resolvedPath != null) {
        return resolvedPath;
      }
    }

    // No valid root directory found - fail
    throw new IOException("No valid jar cache root directory found. Please configure "
        + GobblinYarnConfigurationKeys.JAR_CACHE_ROOT_DIR + " or "
        + GobblinYarnConfigurationKeys.FALLBACK_JAR_CACHE_ROOT_DIR
        + " with a valid directory path, or explicitly set "
        + GobblinYarnConfigurationKeys.JAR_CACHE_DIR);
  }

  /**
   * Validates if the root directory exists and computes the full path with suffix.
   *
   * @param fs the filesystem to check
   * @param rootDir the root directory to validate
   * @param suffix the suffix to append
   * @param configName the config name for logging
   * @return the computed path if valid, null otherwise
   * @throws IOException if filesystem operations fail
   */
  @VisibleForTesting
  static Path validateAndComputePath(FileSystem fs, String rootDir, String suffix, String configName) throws IOException {
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

