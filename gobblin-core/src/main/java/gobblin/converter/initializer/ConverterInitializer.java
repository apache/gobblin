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

package gobblin.converter.initializer;

import gobblin.initializer.Initializer;

/**
 * ConverterInitializer is being invoked at driver which means it will only invoked once per converter.
 * This is to remove any duplication work among task, and any initialization that is same per task can be put in here.
 */
public interface ConverterInitializer extends Initializer {

}