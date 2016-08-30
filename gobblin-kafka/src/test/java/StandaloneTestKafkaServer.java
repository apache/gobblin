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

import lombok.extern.slf4j.Slf4j;

import gobblin.kafka.KafkaTestBase;


/**
 * A standalone test Kafka server, useful for debugging.
 */
@Slf4j
public class StandaloneTestKafkaServer {


  public static void main(String[] args)
      throws InterruptedException {

    final KafkaTestBase kafkaTestBase = new KafkaTestBase();
    System.out.println("Started server on port: " + kafkaTestBase.getKafkaServerPort());
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run()
      {
        log.info("Shutting down...");
        kafkaTestBase.stopServers();
      }
    });
    kafkaTestBase.startServers();


  }
}
