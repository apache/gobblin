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

package org.apache.gobblin.restli.throttling;

/**
 * Versions of the Throttling service protocol. Allows the server to know what the client understands, and to adjust
 * the response based on the client version. Only add new versions at the end.
 */
public enum ThrottlingProtocolVersion {
	/** Base version of throttling server. */
	BASE,
	/** Clients at this level know to wait before distributing permits allocated to them. */
	WAIT_ON_CLIENT
}
