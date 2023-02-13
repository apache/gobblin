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

package org.apache.gobblin.data.management.copy.iceberg;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * Provides an {@link IcebergCatalog}.
 */
public class IcebergCatalogFactory {
  public static IcebergCatalog create(String icebergCatalogClassName, Map<String, String> properties, Configuration configuration) throws IOException {
    try {
      Class<?> icebergCatalogClass = Class.forName(icebergCatalogClassName);
      IcebergCatalog icebergCatalog = (IcebergCatalog) GobblinConstructorUtils.invokeConstructor(icebergCatalogClass, icebergCatalogClassName);
      icebergCatalog.initialize(properties, configuration);
      return icebergCatalog;
    } catch (ReflectiveOperationException ex) {
      throw new IOException(ex);
    }
  }
}
