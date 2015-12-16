/*
 * Copyright (C) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.config.store.api;

/**
 * ConfigStoreWithStableVersion is used to indicate that the configuration store support stable version. 
 * 
 * If the {@ConfigClient} is constructed with policy {@ConfigVersion.VersionStabilityPolicy.RAISE_ERROR},
 * then that {@ConfigClient} can only connected to the {@ConfigStoreWithStableVersion} configuration store.
 * 
 * 
 * Once the version been published to the configuration store, the content will not be changed
 * @author mitu
 *
 */
public interface ConfigStoreWithStableVersion {

}
