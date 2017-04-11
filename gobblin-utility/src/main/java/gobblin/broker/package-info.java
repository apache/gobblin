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
 * A {@link gobblin.broker.iface.SharedResourcesBroker} is an object that provides and manages objects that are accessed
 * from multiple points in the application, allowing disconnected components to use shared objects, as well as easy user
 * configuration for those objects.
 *
 * As a model, consider file handles for emitting logs. Multiple tasks in the application might need to access a global log
 * file, or each task might have its own log file. To use a {@link gobblin.broker.iface.SharedResourcesBroker}, a task
 * creates a factory (see {@link gobblin.broker.iface.SharedResourceFactory}), in this case a log file handle factory.
 * To acquire the file handle, the task sends a request to
 * the broker providing the log file handle factory and a {@link gobblin.broker.iface.SharedResourceKey} (a discriminator between
 * different objects created by the same factory, in the case of the log file handle, the key could specify whether we
 * need an error log handle or an info file handle). The broker has a cache of already created objects, and will either
 * return the same object if one matches the task's request, or will use the factory to create a new object.
 *
 * Brokers and the objects cached in them are scoped (see {@link gobblin.broker.iface.ScopeType} and
 * {@link gobblin.broker.iface.ScopeInstance}). Scoping allows the application to provide information to the broker
 * about its topology, and allows different scopes to get different objects. In the log file handle example, there might
 * be a different handle per task, so all calls withing the same task will get the same handle, while calls from different
 * tasks will get a different broker. In the most common use case, the task need not worry about scopes, as the
 * factory automatically determines which scope the handle should be created on. However, scoped requests are also
 * available, where a task can request an object at a specified scope.
 *
 * When creating a new object, the broker passes a configuration to the factory (see {@link gobblin.broker.iface.ConfigView}
 * and {@link gobblin.broker.iface.ScopedConfigView}), allowing users to globally change the
 * behavior of shared resources transparently to the task. Users can specify configurations for specific factories, scopes,
 * and keys (for example, the location of the log file could be settable through configuration, and user can specify
 * a different location for global and task scope logs).
 */
package gobblin.broker;
