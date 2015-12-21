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

import java.util.Collection;

/**
 * This is an extension of the {@link ConfigStoreWithImportedBy} interface which marks that
 * this {@link ConfigStore} implementation supports not only efficiently obtaining the
 * config keys that directly import a given config key but also the full transitive closure of such
 * keys.
 *
 * Note that when calculating the transitive closure, implicit imports coming from ancestor config
 * keys are also considered.
 */
public interface ConfigStoreWithImportedByRecursively extends ConfigStoreWithImportedBy {
  /**
   * Obtains all config keys which directly or indirectly import a given config key
   * @param  configKey      the path of the config key being imported
   * @param  version        the configuration version to check against
   * @return The {@link Collection} of paths of the config keys that directly or indirectly import
   *         the specified config key in the specified conf version.
   * @throws VersionDoesNotExistException if the requested config version does not exist (any longer)
   */
  public Collection<ConfigKeyPath> getImportedByRecursively(ConfigKeyPath configKey, String version)
      throws VersionDoesNotExistException;
}
