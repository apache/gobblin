/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.options.classes;

import gobblin.util.options.annotations.Checked;


@Checked
public class SimpleClass {

  @gobblin.util.options.annotations.UserOption(required = true)
  public static final String REQUIRED_OPT = SimpleClass.class.getCanonicalName() + ".required";
  @gobblin.util.options.annotations.UserOption
  public static final String OPTIONAL_OPT = SimpleClass.class.getCanonicalName() + ".optional";

}
