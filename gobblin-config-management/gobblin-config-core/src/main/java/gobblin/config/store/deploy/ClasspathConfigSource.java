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
package gobblin.config.store.deploy;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;


/**
 * A {@link DeployableConfigSource} that reads configs to be deployed from classpath.
 * Caller can set {@link #CONFIG_STORE_CLASSPATH_RESOURCE_NAME_KEY} to the name of classpath resource under which deployable configs are available.
 * If the property is not set the {@link #DEFAULT_CONFIG_STORE_CLASSPATH_RESOURCE_NAME} is used to search the classpath for deployable configs
 * <p>
 * It finds the config files in classpath under the
 * <code>classpathRootName</code> directory. Every config file found will be deployed to the <code>storeUri</code> with
 * a new <code>version</code>.
 * </p>
 */
public class ClasspathConfigSource implements DeployableConfigSource {

  private final String classpathRootName;
  private static final String DEFAULT_CONFIG_STORE_CLASSPATH_RESOURCE_NAME = "_CONFIG_STORE";
  public static final String CONFIG_STORE_CLASSPATH_RESOURCE_NAME_KEY =
      "gobblin.config.management.store.deploy.classpathresource";

  /**
   * A {@link DeployableConfigSource} that reads configs to be deployed from classpath.
   * Caller can set {@link #CONFIG_STORE_CLASSPATH_RESOURCE_NAME_KEY} to the name of classpath resource under which deployable configs are available.
   * If the property is not set the {@link #DEFAULT_CONFIG_STORE_CLASSPATH_RESOURCE_NAME} is used to search the classpath for deployable configs
   * <p>
   * It finds the config files in classpath under the
   * <code>classpathRootName</code> directory. Every config file found will be deployed to the <code>storeUri</code> with
   * a new <code>version</code>.
   * </p>
   */
  public ClasspathConfigSource(Properties props) {
    this.classpathRootName =
        props.getProperty(CONFIG_STORE_CLASSPATH_RESOURCE_NAME_KEY, DEFAULT_CONFIG_STORE_CLASSPATH_RESOURCE_NAME);
  }

  /**
   * Scan the classpath for {@link #classpathRootName} and return all resources under it.
   * {@inheritDoc}
   * @see gobblin.config.store.deploy.DeployableConfigSource#getDeployableConfigPaths()
   */
  private Set<String> getDeployableConfigPaths() {

    ConfigurationBuilder cb =
        new ConfigurationBuilder().setUrls(ClasspathHelper.forClassLoader()).setScanners(new ResourcesScanner())
            .filterInputsBy(new FilterBuilder().include(String.format(".*%s.*", this.classpathRootName)));

    Reflections reflections = new Reflections(cb);
    Pattern pattern = Pattern.compile(".*");

    return reflections.getResources(pattern);
  }

  /**
   * Open an {@link InputStream} for <code>resourcePath</code> in classpath
   * {@inheritDoc}
   * @see gobblin.config.store.deploy.DeployableConfigSource#getConfigStream(java.lang.String)
   */
  private static InputStream getConfigStream(String configPath) {
    return ClasspathConfigSource.class.getClassLoader().getResourceAsStream(configPath);
  }

  /**
   * Scan the classpath for {@link #classpathRootName} and opens an {@link InputStream} each resource under it.
   * {@inheritDoc}
   * @see gobblin.config.store.deploy.DeployableConfigSource#getConfigStreams()
   */
  @Override
  public Set<ConfigStream> getConfigStreams() throws IOException {
    Set<ConfigStream> configStreams = Sets.newHashSet();
    for (String configPath : getDeployableConfigPaths()) {
      configStreams.add(new ConfigStream(Optional.of(getConfigStream(configPath)),
          StringUtils.substringAfter(Strings.nullToEmpty(configPath), this.classpathRootName + "/")));
    }
    return configStreams;
  }
}
