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

package gobblin.dataset.config;

/**
 * Since the tag could be tagged with other tags. It is possible to have circular dependency among tags
 * 
 * This exception will signal the circular dependency among tags.
 * @author mitu
 *
 */

public class TagCircularDependencyException extends RuntimeException{

  /**
   * 
   */
  private static final long serialVersionUID = -164765448729513949L;
  public TagCircularDependencyException(String message){
    super(message);
  }
}
