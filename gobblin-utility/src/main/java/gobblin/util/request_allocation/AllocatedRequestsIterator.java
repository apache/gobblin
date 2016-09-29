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

package gobblin.util.request_allocation;

import java.util.Iterator;


/**
 * An {@link Iterator} over {@link Request} that also provides with the total resources used by all consumed entries.
 */
public interface AllocatedRequestsIterator<T extends Request<T>> extends Iterator<T> {
  /**
   * @return The total resources used by the elements consumed so far from this iterator.
   */
  ResourceRequirement totalResourcesUsed();
}
