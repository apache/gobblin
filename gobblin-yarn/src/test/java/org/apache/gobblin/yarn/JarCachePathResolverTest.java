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
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;


/**
 * Tests for {@link JarCachePathResolver}.
 */
public class JarCachePathResolverTest {


  @Test
  public void testResolveJarCachePath_ExplicitJarCacheDir() throws IOException {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    String explicitCacheDir = "/explicit/cache/dir";
    
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_DIR, ConfigValueFactory.fromAnyRef(explicitCacheDir))
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, 
            ConfigValueFactory.fromAnyRef(System.currentTimeMillis()));
    
    Path result = JarCachePathResolver.resolveJarCachePath(config, mockFs);
    
    // Should use explicitly configured JAR_CACHE_DIR
    Assert.assertEquals(result.toString(), explicitCacheDir);
    // Verify no filesystem checks were made (explicit config is used as-is)
    Mockito.verify(mockFs, Mockito.never()).exists(Mockito.any(Path.class));
  }

  @Test
  public void testResolveJarCachePath_RootDirExists() throws IOException {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    String rootDir = "/user/testuser";
    String suffix = ".gobblinCache/gobblin-temporal/myproject";
    String expectedFullPath = "/user/testuser/.gobblinCache/gobblin-temporal/myproject";
    
    // Mock: Root directory exists
    Mockito.when(mockFs.exists(Mockito.any(Path.class))).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) {
        Path path = invocation.getArgument(0);
        return path.toString().equals(rootDir);
      }
    });
    
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ROOT_DIR, ConfigValueFactory.fromAnyRef(rootDir))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_SUFFIX, ConfigValueFactory.fromAnyRef(suffix))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true))
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, 
            ConfigValueFactory.fromAnyRef(System.currentTimeMillis()));
    
    Path result = JarCachePathResolver.resolveJarCachePath(config, mockFs);
    
    // Should resolve to root + suffix
    Assert.assertEquals(result.toString(), expectedFullPath);
  }

  @Test
  public void testResolveJarCachePath_RootDirNotExistsFallbackRootExists() throws IOException {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    String rootDir = "/user/baduser";
    String fallbackRootDir = "/user/gooduser";
    String suffix = ".gobblinCache/gobblin-temporal/myproject";
    String expectedFullPath = "/user/gooduser/.gobblinCache/gobblin-temporal/myproject";
    
    // Mock: Root dir doesn't exist, but fallback root exists
    Mockito.when(mockFs.exists(Mockito.any(Path.class))).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) {
        Path path = invocation.getArgument(0);
        // Only fallback root directory exists
        return path.toString().equals(fallbackRootDir);
      }
    });
    
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ROOT_DIR, ConfigValueFactory.fromAnyRef(rootDir))
        .withValue(GobblinYarnConfigurationKeys.FALLBACK_JAR_CACHE_ROOT_DIR, ConfigValueFactory.fromAnyRef(fallbackRootDir))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_SUFFIX, ConfigValueFactory.fromAnyRef(suffix))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true))
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, 
            ConfigValueFactory.fromAnyRef(System.currentTimeMillis()));
    
    Path result = JarCachePathResolver.resolveJarCachePath(config, mockFs);
    
    // Should resolve to fallback root + suffix
    Assert.assertEquals(result.toString(), expectedFullPath);
  }

  @Test(expectedExceptions = IOException.class, 
        expectedExceptionsMessageRegExp = ".*No valid jar cache root directory found.*")
  public void testResolveJarCachePath_NeitherRootDirExists() throws IOException {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    String rootDir = "/user/baduser1";
    String fallbackRootDir = "/user/baduser2";
    String suffix = ".gobblinCache/gobblin-temporal/myproject";
    
    // Mock: Neither root directory exists
    Mockito.when(mockFs.exists(Mockito.any(Path.class))).thenReturn(false);
    
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ROOT_DIR, ConfigValueFactory.fromAnyRef(rootDir))
        .withValue(GobblinYarnConfigurationKeys.FALLBACK_JAR_CACHE_ROOT_DIR, ConfigValueFactory.fromAnyRef(fallbackRootDir))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_SUFFIX, ConfigValueFactory.fromAnyRef(suffix))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true))
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, 
            ConfigValueFactory.fromAnyRef(System.currentTimeMillis()));
    
    // Should throw IOException when no valid root directory found
    JarCachePathResolver.resolveJarCachePath(config, mockFs);
  }

  @Test
  public void testResolveJarCachePath_OnlyFallbackRootConfigured() throws IOException {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    String fallbackRootDir = "/user/testuser";
    String suffix = ".gobblinCache/gobblin-temporal/myproject";
    String expectedFullPath = "/user/testuser/.gobblinCache/gobblin-temporal/myproject";
    
    // Mock: Fallback root directory exists
    Mockito.when(mockFs.exists(Mockito.any(Path.class))).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) {
        Path path = invocation.getArgument(0);
        return path.toString().equals(fallbackRootDir);
      }
    });
    
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.FALLBACK_JAR_CACHE_ROOT_DIR, ConfigValueFactory.fromAnyRef(fallbackRootDir))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_SUFFIX, ConfigValueFactory.fromAnyRef(suffix))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true))
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, 
            ConfigValueFactory.fromAnyRef(System.currentTimeMillis()));
    
    Path result = JarCachePathResolver.resolveJarCachePath(config, mockFs);
    
    // Should resolve to fallback root + suffix
    Assert.assertEquals(result.toString(), expectedFullPath);
  }

  @Test(expectedExceptions = IOException.class,
        expectedExceptionsMessageRegExp = ".*No valid jar cache root directory found.*")
  public void testResolveJarCachePath_NoRootDirsConfigured() throws IOException {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true))
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, 
            ConfigValueFactory.fromAnyRef(System.currentTimeMillis()));
    
    // Should throw IOException when no root directories are configured
    JarCachePathResolver.resolveJarCachePath(config, mockFs);
  }

  @Test
  public void testResolveJarCachePath_DefaultSuffix() throws IOException {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    String rootDir = "/user/testuser";
    // Note: Hadoop Path normalizes and removes trailing slashes
    String expectedFullPath = "/user/testuser/.gobblinCache/gobblin-temporal";
    
    // Mock: Root directory exists
    Mockito.when(mockFs.exists(Mockito.any(Path.class))).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) {
        Path path = invocation.getArgument(0);
        return path.toString().equals(rootDir);
      }
    });
    
    // Config without JAR_CACHE_SUFFIX - should use default
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ROOT_DIR, ConfigValueFactory.fromAnyRef(rootDir))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true))
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, 
            ConfigValueFactory.fromAnyRef(System.currentTimeMillis()));
    
    Path result = JarCachePathResolver.resolveJarCachePath(config, mockFs);
    
    // Should use default suffix
    Assert.assertEquals(result.toString(), expectedFullPath);
  }

  @Test
  public void testResolveJarCachePath_EmptySuffixUsesDefault() throws IOException {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    String rootDir = "/user/testuser";
    // Note: Hadoop Path normalizes and removes trailing slashes
    String expectedFullPath = "/user/testuser/.gobblinCache/gobblin-temporal";
    
    // Mock: Root directory exists
    Mockito.when(mockFs.exists(Mockito.any(Path.class))).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) {
        Path path = invocation.getArgument(0);
        return path.toString().equals(rootDir);
      }
    });
    
    // Config with empty JAR_CACHE_SUFFIX - should use default
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ROOT_DIR, ConfigValueFactory.fromAnyRef(rootDir))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_SUFFIX, ConfigValueFactory.fromAnyRef(""))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true))
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, 
            ConfigValueFactory.fromAnyRef(System.currentTimeMillis()));
    
    Path result = JarCachePathResolver.resolveJarCachePath(config, mockFs);
    
    // Should use default suffix when configured suffix is empty
    Assert.assertEquals(result.toString(), expectedFullPath);
  }

  @Test
  public void testResolveJarCachePath_SuffixNormalization() throws IOException {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    String rootDir = "/user/testuser";
    String suffixWithLeadingSlash = "/.gobblinCache/gobblin-temporal/myproject";
    String expectedFullPath = "/user/testuser/.gobblinCache/gobblin-temporal/myproject";
    
    // Mock: Root directory exists
    Mockito.when(mockFs.exists(Mockito.any(Path.class))).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) {
        Path path = invocation.getArgument(0);
        return path.toString().equals(rootDir);
      }
    });
    
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ROOT_DIR, ConfigValueFactory.fromAnyRef(rootDir))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_SUFFIX, ConfigValueFactory.fromAnyRef(suffixWithLeadingSlash))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true))
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, 
            ConfigValueFactory.fromAnyRef(System.currentTimeMillis()));
    
    Path result = JarCachePathResolver.resolveJarCachePath(config, mockFs);
    
    // Should normalize suffix by stripping leading '/' to avoid absolute path issue
    Assert.assertEquals(result.toString(), expectedFullPath);
  }

}

