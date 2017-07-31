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

package org.apache.gobblin.hive;

import lombok.Getter;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import com.google.common.base.Optional;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;


/**
 * An implementation of {@link BasePooledObjectFactory} for {@link IMetaStoreClient}.
 */
public class HiveMetaStoreClientFactory extends BasePooledObjectFactory<IMetaStoreClient> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreClientFactory.class);

  public static final String HIVE_METASTORE_TOKEN_SIGNATURE = "hive.metastore.token.signature";
  @Getter
  private HiveConf hiveConf;

  public HiveMetaStoreClientFactory(Optional<String> hcatURI) {
    this(getHiveConf(hcatURI));
  }

  private static HiveConf getHiveConf(Optional<String> hcatURI) {
    HiveConf hiveConf = new HiveConf();
    if (hcatURI.isPresent() && StringUtils.isNotBlank(hcatURI.get())) {
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hcatURI.get());
      hiveConf.set(HIVE_METASTORE_TOKEN_SIGNATURE, hcatURI.get());
    }
    return hiveConf;
  }

  public HiveMetaStoreClientFactory(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  public HiveMetaStoreClientFactory() {
    this(Optional.<String> absent());
  }

  private IMetaStoreClient createMetaStoreClient() throws MetaException {
    HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
      @Override
      public HiveMetaHook getHook(Table tbl) throws MetaException {
        if (tbl == null) {
          return null;
        }

        try {
          HiveStorageHandler storageHandler =
              HiveUtils.getStorageHandler(hiveConf, tbl.getParameters().get(META_TABLE_STORAGE));
          return storageHandler == null ? null : storageHandler.getMetaHook();
        } catch (HiveException e) {
          LOG.error(e.toString());
          throw new MetaException("Failed to get storage handler: " + e);
        }
      }
    };

    return RetryingMetaStoreClient.getProxy(hiveConf, hookLoader, HiveMetaStoreClient.class.getName());
  }

  @Override
  public IMetaStoreClient create() {
    try {
      return createMetaStoreClient();
    } catch (MetaException e) {
      throw new RuntimeException("Unable to create " + IMetaStoreClient.class.getSimpleName(), e);
    }
  }

  @Override
  public PooledObject<IMetaStoreClient> wrap(IMetaStoreClient client) {
    return new DefaultPooledObject<>(client);
  }

  @Override
  public void destroyObject(PooledObject<IMetaStoreClient> client) {
    client.getObject().close();
  }

}
