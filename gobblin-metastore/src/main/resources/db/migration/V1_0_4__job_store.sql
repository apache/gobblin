--
--  Licensed to the Apache Software Foundation (ASF) under one or more
--  contributor license agreements.  See the NOTICE file distributed with
--  this work for additional information regarding copyright ownership.
--  The ASF licenses this file to You under the Apache License, Version 2.0
--  (the "License"); you may not use this file except in compliance with
--  the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.
--

CREATE TABLE gobblin_job_owners
(
    name         varchar(255) NOT NULL,
    email        varchar(255) NOT NULL,
    team_name    int(11)               DEFAULT NULL,
    team_email   int(11)               DEFAULT NULL,
    org_name     int(11)               DEFAULT NULL,
    created_date timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_date timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (email)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE gobblin_sync_systems
(
    name         varchar(255) NOT NULL,
    db_type      varchar(255) NOT NULL,
    users        varchar(255) NOT NULL,
    on_hold      tinyint(1)   NOT NULL DEFAULT '0',
    deprecated   tinyint(1)   NOT NULL DEFAULT '0',
    config       text,
    created_date timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_date timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (name)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE gobblin_jobs
(
    id            int(11)      NOT NULL AUTO_INCREMENT,
    name          varchar(255) NOT NULL,
    description   varchar(255)          DEFAULT NULL,
    schedule      varchar(64)           DEFAULT NULL,
    is_disabled   tinyint(1)            DEFAULT '0',
    priority      smallint              DEFAULT 100,
    configs       text,
    owner_email   varchar(255) NOT NULL,
    source_system varchar(255) NOT NULL,
    #source_dataset varchar(255)          DEFAULT NULL,
    #target_dataset varchar(255)          DEFAULT NULL,
    target_system varchar(255) NOT NULL,
    created_date  timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_date  timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (name),
#     FOREIGN KEY (owner_email) REFERENCES gobblin_job_owners (email),
#     FOREIGN KEY (source_system) REFERENCES gobblin_sync_systems (name),
#     FOREIGN KEY (target_system) REFERENCES gobblin_sync_systems (name),
    INDEX (name),
    INDEX (id)
) AUTO_INCREMENT = 1
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;


CREATE TABLE gobblin_sync_systems_maintenance
(
    id               int(11)      NOT NULL AUTO_INCREMENT,
    sync_system_name varchar(255) NOT NULL,
    type             varchar(255) NOT NULL,
    start_time       datetime     NOT NULL,
    end_time         datetime     NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (sync_system_name) REFERENCES gobblin_sync_systems (name)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;