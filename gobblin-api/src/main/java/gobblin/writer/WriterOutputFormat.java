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

package gobblin.writer;

import org.apache.commons.lang3.StringUtils;

/**
 * An enumeration of writer output formats.
 *
 * @author Yinan Li
 */
public enum WriterOutputFormat {
  AVRO("avro"),
  PARQUET("parquet"),
  PROTOBUF("protobuf"),
  JSON("json"),
  ORC("orc"),
  CSV("csv"),
  OTHER(StringUtils.EMPTY);

  /**
   * Extension specifies the file name extension
   */
  private final String extension;

  WriterOutputFormat(String extension) {
    this.extension = extension;
  }

  /**
   * Returns the file name extension for the enum type
   * @return a string representation of the file name extension
   */
  public String getExtension() {
    return this.extension;
  }
}
