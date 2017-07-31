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

package org.apache.gobblin.runtime.spec_catalog;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import org.apache.gobblin.runtime.api.SpecCatalogListener;
import org.apache.gobblin.runtime.api.SpecCatalogListenersContainer;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.util.callbacks.CallbacksDispatcher;


public class SpecCatalogListenersList implements SpecCatalogListener, SpecCatalogListenersContainer, Closeable {
  private final CallbacksDispatcher<SpecCatalogListener> _disp;

  public SpecCatalogListenersList() {
    this(Optional.<Logger>absent());
  }

  public SpecCatalogListenersList(Optional<Logger> log) {
    _disp = new CallbacksDispatcher<SpecCatalogListener>(Optional.<ExecutorService>absent(), log);
  }

  public Logger getLog() {
    return _disp.getLog();
  }

  public synchronized List<SpecCatalogListener> getListeners() {
    return _disp.getListeners();
  }

  @Override
  public synchronized void addListener(SpecCatalogListener newListener) {
    _disp.addListener(newListener);
  }

  @Override
  public synchronized void removeListener(SpecCatalogListener oldListener) {
    _disp.removeListener(oldListener);
  }

  @Override
  public synchronized void onAddSpec(Spec addedSpec) {
    Preconditions.checkNotNull(addedSpec);
    try {
      _disp.execCallbacks(new SpecCatalogListener.AddSpecCallback(addedSpec));
    } catch (InterruptedException e) {
      getLog().warn("onAddSpec interrupted.");
    }
  }

  @Override
  public synchronized void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion) {
    Preconditions.checkNotNull(deletedSpecURI);

    try {
      _disp.execCallbacks(new SpecCatalogListener.DeleteSpecCallback(deletedSpecURI, deletedSpecVersion));
    } catch (InterruptedException e) {
      getLog().warn("onDeleteSpec interrupted.");
    }
  }

  @Override
  public synchronized void onUpdateSpec(Spec updatedSpec) {
    Preconditions.checkNotNull(updatedSpec);
    try {
      _disp.execCallbacks(new UpdateSpecCallback(updatedSpec));
    } catch (InterruptedException e) {
      getLog().warn("onUpdateSpec interrupted.");
    }
  }

  @Override
  public void close()
      throws IOException {
    _disp.close();
  }

  public void callbackOneListener(Function<SpecCatalogListener, Void> callback,
      SpecCatalogListener listener) {
    try {
      _disp.execCallbacks(callback, listener);
    } catch (InterruptedException e) {
      getLog().warn("callback interrupted: "+ callback);
    }
  }

  @Override
  public void registerWeakSpecCatalogListener(SpecCatalogListener specListener) {
    _disp.addWeakListener(specListener);
  }

}
