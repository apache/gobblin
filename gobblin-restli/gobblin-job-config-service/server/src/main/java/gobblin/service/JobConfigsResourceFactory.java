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

package gobblin.service;

import gobblin.runtime.api.MutableJobCatalog;
import java.lang.reflect.Constructor;
import java.util.Map;

import com.linkedin.restli.internal.server.model.ResourceModel;
import com.linkedin.restli.server.resources.ResourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.Validate;


/**
 * JobConfigs Resource Factory that is used to create the JobConfigs restli resources.
 */
@Slf4j
public class JobConfigsResourceFactory implements ResourceFactory {
  private final MutableJobCatalog jobCatalog;

  public JobConfigsResourceFactory(MutableJobCatalog jobCatalog) {
    this.jobCatalog = jobCatalog;
  }

  @Override
  public void setRootResources(Map<String, ResourceModel> rootResources) {
  }

  /**
   * Create an instance of a class with arguments
   * @param clazz Class name
   * @param args arguments for constructor
   * @return
   */
  private <T> T createInstance(String clazz, Object... args) {
    Validate.notNull(clazz, "null class name");
    try {
      Class<T> classObj = (Class<T>) Class.forName(clazz);
      Class<?>[] argTypes = new Class<?>[args.length];
      for (int i = 0; i < args.length; i++) {
        argTypes[i] = args[i].getClass();
      }
      Constructor<T> ctor = classObj.getDeclaredConstructor(argTypes);
      return ctor.newInstance(args);
    } catch (Exception e) {
      log.warn("Failed to create instance for: " + clazz, e);
      return null;
    }
  }

  @Override
  public <R> R create(Class<R> resourceClass) {
    return createInstance(resourceClass.getCanonicalName(), jobCatalog);
  }
}
