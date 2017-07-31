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

package org.apache.gobblin.runtime.api;

import java.net.URI;

import com.google.common.base.Objects;

import org.apache.gobblin.util.callbacks.Callback;

public interface SpecCatalogListener {
  /** Invoked when a new {@link Spec} is added to the catalog and for all pre-existing specs on registration
   * of the listener.*/
  void onAddSpec(Spec addedSpec);

  /**
   * Invoked when a {@link Spec} gets removed from the catalog.
   */
  public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion);

  /**
   * Invoked when the contents of a {@link Spec} gets updated in the catalog.
   */
  public void onUpdateSpec(Spec updatedSpec);

  /** A standard implementation of onAddSpec as a functional object */
  public static class AddSpecCallback extends Callback<SpecCatalogListener, Void> {
    private final Spec _addedSpec;
    public AddSpecCallback(Spec addedSpec) {
      super(Objects.toStringHelper("onAddSpec").add("addedSpec", addedSpec).toString());
      _addedSpec = addedSpec;
    }

    @Override public Void apply(SpecCatalogListener listener) {
      listener.onAddSpec(_addedSpec);
      return null;
    }
  }

  /** A standard implementation of onDeleteSpec as a functional object */
  public static class DeleteSpecCallback extends Callback<SpecCatalogListener, Void> {
    private final URI _deletedSpecURI;
    private final String _deletedSpecVersion;

    public DeleteSpecCallback(URI deletedSpecURI, String deletedSpecVersion) {
      super(Objects.toStringHelper("onDeleteSpec")
          .add("deletedSpecURI", deletedSpecURI)
          .add("deletedSpecVersion", deletedSpecVersion)
          .toString());
      _deletedSpecURI = deletedSpecURI;
      _deletedSpecVersion = deletedSpecVersion;
    }

    @Override public Void apply(SpecCatalogListener listener) {
      listener.onDeleteSpec(_deletedSpecURI, _deletedSpecVersion);
      return null;
    }
  }

  public static class UpdateSpecCallback extends Callback<SpecCatalogListener, Void> {
    private final Spec _updatedSpec;
    public UpdateSpecCallback(Spec updatedSpec) {
      super(Objects.toStringHelper("onUpdateSpec")
          .add("updatedSpec", updatedSpec).toString());
      _updatedSpec = updatedSpec;
    }

    @Override
    public Void apply(SpecCatalogListener listener) {
      listener.onUpdateSpec(_updatedSpec);
      return null;
    }
  }
}
