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

/**
 * This package provides some simple instrumentation to handling callback execution.
 *
 * <dl>
 *    <dt>Listeners</dt>
 *    <dd>Listeners are the object to which the callbacks are sent. This package does not impose
 *    too many restrictions on what listeners should look like. For a given
 *    {@link org.apache.gobblin.util.callbacks.CallbacksDispatcher}, they should all implement the same
 *    interface. </dd>
 *    <dt>Callbacks</dt>
 *    <dd>Callbacks are represented as {@link com.google.common.base.Function}<L, R> instances which
 *    take one L parameter, the listener to be applied on, and can return a result of type R. If no
 *    meaningful result is returned, R should be Void. There is a helper class
 *    {@link org.apache.gobblin.util.callbacks.Callback} which allows to assign a meaningful string to the
 *    callback. Typically, this is the name of the callback and short description of any bound
 *    arguments.
 *
 *    <p>Note that callback instances as defined above can't take any arguments to be passed to the
 *    listeners. They should be viewed as a binding of the actual callback method call to a specific
 *    set of arguments.
 *
 *    <p>For example, if we want to define a callback <code>void onNewJob(JobSpec newJob)</code> to
 *    be sent to JobCatalogListener interface, we need:
 *    <ul>
 *      <li>Define the <code>NewJobCallback implements Callback<JobCatalogListener, Void> </code>
 *      <li>In the constructor, the above class should take and save a parameter for the JobSpec.
 *      <li>The apply method, should look something like <code>input.onNewJob(this.newJob)</code>
 *    </ul>
 *    </dd>
 *    <dt>Callbacks Dispatcher</dt>
 *    <dd> The {@link org.apache.gobblin.util.callbacks.CallbacksDispatcher}<L> is responsible for:
 *       <ul>
 *          <li>Maintaining a list of listeners of type L.
 *          <li>Dispatch callbacks invoked through {@link org.apache.gobblin.util.callbacks.CallbacksDispatcher#execCallbacks(com.google.common.base.Function)}
 *             to each of the above listeners.
 *          <li>Provide parallelism of the callbacks if necessary.
 *          <li>Guarantee isolation of failures in callbacks.
 *          <li>Provide logging for debugging purposes.
 *          <li>Classify callback results in {@link org.apache.gobblin.util.callbacks.CallbacksDispatcher.CallbackResults}
 *          for easier upstream processing.
 *       </ul>
 *    </dd>
 *
 * </dl>
 */
package org.apache.gobblin.util.callbacks;
