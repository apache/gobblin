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

package gobblin.source.workunit;

import java.util.List;

/**
 * A bin packing algorithm for packing {@link WorkUnit}s into {@link MultiWorkUnit}s.
 */
public interface WorkUnitBinPacker {

  /**
   * Packs the input {@link WorkUnit}s into {@link MultiWorkUnit}s.
   * @param workUnitsIn List of {@link WorkUnit}s to pack.
   * @param weighter {@link WorkUnitWeighter} that provides weights for {@link WorkUnit}s.
   */
  public List<WorkUnit> pack(List<WorkUnit> workUnitsIn, WorkUnitWeighter weighter);

}
