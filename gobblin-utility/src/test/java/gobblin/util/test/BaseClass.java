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

package gobblin.util.test;

import lombok.EqualsAndHashCode;

import java.util.Random;


/**
 * Used for {@link gobblin.util.io.GsonInterfaceAdapterTest}.
 */
@EqualsAndHashCode
public class BaseClass {

  public BaseClass() {
    this.field = Integer.toString(new Random().nextInt());
  }

  private String field;

}
