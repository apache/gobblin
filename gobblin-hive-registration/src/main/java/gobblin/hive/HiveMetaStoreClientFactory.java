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

package gobblin.hive;

import lombok.Getter;

import java.io.IOException;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import com.google.common.base.Optional;


/**
 * An implementation of {@link BasePooledObjectFactory} for {@link IMetaStoreClient}.
 *
 * @author ziliu
 */
public class HiveMetaStoreClientFactory extends BasePooledObjectFactory<IMetaStoreClient> {

  @Getter
  private HiveConf hiveConf;

  public HiveMetaStoreClientFactory(Optional<String> hcatURI) throws IOException {
    this(getHiveConf(hcatURI));
  }

  private static HiveConf getHiveConf(Optional<String> hcatURI) {
    HiveConf hiveConf = new HiveConf();
    if (hcatURI.isPresent()) {
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hcatURI.get());
    }
    return hiveConf;
  }

  public HiveMetaStoreClientFactory(HiveConf hiveConf) throws IOException {
    this.hiveConf = hiveConf;
  }

  public HiveMetaStoreClientFactory() throws IOException {
    this(Optional.<String>absent());
  }

  @Override
  public IMetaStoreClient create() {
    try {
      return new HiveMetaStoreClient(this.hiveConf);
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
