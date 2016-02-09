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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import gobblin.annotation.Alpha;
import gobblin.config.client.api.VersionStabilityPolicy;

/**
 * The ConfigStoreWithStableVersioning annotation is used to indicate that the configuration store
 * supports stable versioning. This means that:
 *
 * <ul>
 *   <li>Once published the version will remain available for at least a day even if it gets rolled
 *       back.</li>
 *   <li>The version and all its configuration objects are immutable.</li>
 * </ul>
 *
 * Version stability defines the possible outcomes for repeated calls to
 * {@link ConfigStore#getOwnConfig(ConfigKeyPath, String)}
 * for the same config key and version from the same or different JVMs. This is used in conjunction
 * with {@link VersionStabilityPolicy} to control client library behavior with respect to caching
 * config values.
 *
 * @author mitu
 *
 */
//@Documented @Retention(value=RetentionPolicy.RUNTIME) @Target(value=ElementType.TYPE)
@Alpha
public interface ConfigStoreWithStableVersioning extends ConfigStore {

}
