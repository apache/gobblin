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

package gobblin.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;

import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PortUtils {
  public static final int MINIMUM_PORT = 1025;
  public static final int MAXIMUM_PORT = 65535;
  private static final Pattern PORT_REGEX =
          Pattern.compile("\\$\\{PORT_(?>(?>\\?(\\d+))|(?>(\\d+)\\?)|(\\d+|\\?))\\}");
  private final PortLocator portLocator;
  private final ConcurrentMap<Integer, Boolean> assignedPorts;

  public PortUtils() {
    this(new ServerSocketPortLocator());
  }

  @VisibleForTesting
  PortUtils(PortLocator locator) {
    this.portLocator = locator;
    this.assignedPorts = Maps.newConcurrentMap();
  }

  /**
   * Replaces any port tokens in the specified string.
   *
   * NOTE: Tokens can be in the following forms:
   *   1. ${PORT_123}
   *   2. ${PORT_?123}
   *   3. ${PORT_123?}
   *   4. ${PORT_?}
   *
   * @param value The string in which to replace port tokens.
   * @return The replaced string.
   */
  public String replacePortTokens(String value) {
    BiMap<String, Optional<Integer>> portMappings = HashBiMap.create();
    Matcher regexMatcher = PORT_REGEX.matcher(value);
    while (regexMatcher.find()) {
      String token = regexMatcher.group(0);
      if (!portMappings.containsKey(token)) {
        Optional<Integer> portStart = Optional.absent();
        Optional<Integer> portEnd = Optional.absent();
        String unboundedStart = regexMatcher.group(1);
        if (unboundedStart != null) {
          int requestedEndPort = Integer.parseInt(unboundedStart);
          Preconditions.checkArgument(requestedEndPort <= PortUtils.MAXIMUM_PORT);
          portEnd = Optional.of(requestedEndPort);
        } else {
          String unboundedEnd = regexMatcher.group(2);
          if (unboundedEnd != null) {
            int requestedStartPort = Integer.parseInt(unboundedEnd);
            Preconditions.checkArgument(requestedStartPort >= PortUtils.MINIMUM_PORT);
            portStart = Optional.of(requestedStartPort);
          } else {
            String absolute = regexMatcher.group(3);
            if (!"?".equals(absolute)) {
              int requestedPort = Integer.parseInt(absolute);
              Preconditions.checkArgument(requestedPort >= PortUtils.MINIMUM_PORT &&
                      requestedPort <= PortUtils.MAXIMUM_PORT);
              portStart = Optional.of(requestedPort);
              portEnd = Optional.of(requestedPort);
            }
          }
        }
        Optional<Integer> port = takePort(portStart, portEnd);
        portMappings.put(token, port);
      }
    }
    for (Map.Entry<String, Optional<Integer>> port : portMappings.entrySet()) {
      if (port.getValue().isPresent()) {
        value = value.replace(port.getKey(), port.getValue().get().toString());
      }
    }
    return value;
  }

  /**
   * Finds an open port. {@param portStart} and {@param portEnd} can be absent
   *
   * ______________________________________________________
   * | portStart | portEnd  | takenPort                   |
   * |-----------|----------|-----------------------------|
   * |  absent   | absent   | random                      |
   * |  absent   | provided | 1024 < port <= portEnd      |
   * |  provided | absent   | portStart <= port <= 65535  |
   * |  provided | provided | portStart = port = portEnd  |
   * ------------------------------------------------------
   *
   * @param portStart the inclusive starting port
   * @param portEnd the inclusive ending port
   * @return The selected open port.
   */
  private synchronized Optional<Integer> takePort(Optional<Integer> portStart, Optional<Integer> portEnd) {
    if (!portStart.isPresent() && !portEnd.isPresent()) {
      for (int i = 0; i < 65535; i++) {
        try {
          int port = this.portLocator.random();
          Boolean wasAssigned = assignedPorts.putIfAbsent(port, true);
          if (wasAssigned == null || !wasAssigned) {
            return Optional.of(port);
          }
        } catch (Exception ignored) {
        }
      }
    }

    for (int port = portStart.or(MINIMUM_PORT); port <= portEnd.or(MAXIMUM_PORT); port++) {
      try {
        this.portLocator.specific(port);
        Boolean wasAssigned = assignedPorts.putIfAbsent(port, true);
        if (wasAssigned == null || !wasAssigned) {
          return Optional.of(port);
        }
      } catch (Exception ignored) {
      }
    }

    throw new RuntimeException(String.format("No open port could be found for %s to %s", portStart, portEnd));
  }

  @VisibleForTesting
  interface PortLocator {
    int random() throws Exception;
    int specific(int port) throws Exception;
  }

  private static class ServerSocketPortLocator implements PortLocator {
    @Override
    public int random() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
          return serverSocket.getLocalPort();
        }
    }

    @Override
    public int specific(int port) throws Exception {
      try (ServerSocket serverSocket = new ServerSocket(port)) {
        return serverSocket.getLocalPort();
      }
    }
  }
}
