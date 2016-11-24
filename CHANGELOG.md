# Change Log
All notable changes to this project will be documented in this file.

## [Unreleased]
### Added
- Basic checks on PNDA message after deserialization
### Changed
- Externalized build logic from Jenkins to shell script so it can be reused
- Bumped Cloudera libraries to 5.9.0

## [0.1.1] 2016-09-13
### Changed
- Enhanced CI support

## [0.1.0] 2016-07-04
### Added 
* A new converter to convert messages read from Kafka to the PNDA Avro schema (gobblin.pnda.PNDAConverter)
* A new writer writing data with the kitesdk library (gobblin.pnda.PNDAKiteWriterBuilder)
