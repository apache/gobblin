/* (c) 2014 LinkedIn Corp. All rights reserved.
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

import gobblin.configuration.State;


/**
 * A class representing a destination for a writer to write to.
 * It currently supports HDFS and Kafka as destinations.
 *
 * @author ynli
 */
public class Destination {

  /**
   * Enumeration of supported destination types.
   */
  public static enum DestinationType {
    HDFS,
    KAFKA
  }

  // Type of destination
  private final DestinationType type;

  // Destination properties
  private final State properties;

  private Destination(DestinationType type, State properties) {
    this.type = type;
    this.properties = properties;
  }

  /**
   * Get the destination type.
   *
   * @return destination type
   */
  public DestinationType getType() {
    return this.type;
  }

  /**
   * Get configuration properties for the destination type.
   *
   * @return configuration properties
   */
  public State getProperties() {
    return this.properties;
  }

  /**
   * Create a new {@link Destination} instance.
   *
   * @param type destination type
   * @param properties destination properties
   * @return newly created {@link Destination} instance
   */
  public static Destination of(DestinationType type, State properties) {
    return new Destination(type, properties);
  }
}
