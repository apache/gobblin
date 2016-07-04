# [PNDA](http://pnda.io) Fork

This fork adds the following:

* A new converter to convert messages read from Kafka to the PNDA Avro schema (gobblin.pnda.PNDAConverter)
* A new writer writing data with the kitesdk library (gobblin.pnda.PNDAKiteWriterBuilder)

The converter can only work with a Kafka-compatible source. The workflow is:

    KafkaSimpleSource ► PNDAConverter ► [SchemaRowCheckPolicy] ► PNDAKiteWriter

## Build

Gobblin needs to be compiled against CDH hadoop dependencies with the right version:

    $ ./gradlew clean build -PuseHadoop2 -PhadoopVersion=2.6.0-cdh5.4.9

## Configuration

### Kite datasets

Two kite datasets need to be created.

The first one is the main dataset where all the valid PNDA messages are stored. The schema is:

    {"namespace": "pnda.entity",
     "type": "record",
     "name": "event",
     "fields": [
         {"name": "timestamp", "type": "long"},
         {"name": "src",       "type": "string"},
         {"name": "host_ip",   "type": "string"},
         {"name": "rawdata",   "type": "bytes"}
     ]
    }

The second one is where all data in the wrong format are stored. The schema is:

    {"namespace": "pnda.entity",
     "type"     : "record",
     "name"     : "DeserializationError",
     "fields"   : [
       {"name": "topic",     "type": "string"},
       {"name": "timestamp", "type": "long"},
       {"name": "reason",    "type": ["string", "null"]},
       {"name": "payload",   "type": "bytes"}
     ]
    }

See the [Kite CLI reference](http://kitesdk.org/docs/1.1.0/cli-reference.html) for information on creating a Kite dataset. 

For example, to create the first dataset with the following partition strategy (`partition.json` file):

    [
        {"type": "identity", "source": "src", "name": "source"},
        {"type": "year",     "source": "timestamp"},
        {"type": "month",    "source": "timestamp"},
        {"type": "day",      "source": "timestamp"},
        {"type": "hour",     "source": "timestamp"}
    ]

Then execute the following command:

    $ kite-dataset create --schema pnda.avsc dataset:hdfs://192.168.0.227:8020/tmp/PNDA_datasets/master --partition-by partition.json

### Property file

The `source.schema` property must be set to deserialize AVRO data coming from kafka:

    source.schema={"namespace": "pnda.entity", \
                   "type": "record",                            \
                   "name": "event",                             \
                   "fields": [                                  \
                       {"name": "timestamp", "type": "long"},   \
                       {"name": "src",       "type": "string"}, \
                       {"name": "host_ip",   "type": "string"}, \
                       {"name": "rawdata",   "type": "bytes"}   \
                   ]                                            \
                  }

It can also be a the filename of an AVRO schema.

The `converter.classes` property must include the `gobblin.pnda.PNDAConverter` class.
`PNDA.quarantine.dataset.uri` is optional. If not set, wrong messages will be discarded.

    converter.classes=gobblin.pnda.PNDAConverter
    PNDA.quarantine.dataset.uri=dataset:hdfs://192.168.0.227:8020/tmp/PNDA_datasets/quarantine


The `writer.builder.class` property must be set to `gobblin.pnda.PNDAKiteWriterBuilder` to enable the Kite writer.
`kite.writer.dataset.uri` is mandatory.

    writer.builder.class=gobblin.pnda.PNDAKiteWriterBuilder
    kite.writer.dataset.uri=dataset:hdfs://192.168.0.227:8020/tmp/PNDA_datasets/master

## PNDA Dataset creation

In the context of the PNDA platform, Gobblin is normally run on a regular basis every 30 minutes. Datasets are created dynamically on HDFS by discovering available topics in Kafka.

It's important to note that datasets are not created by topics, but dependending on the `src` field in PNDA Avro messages.

Datasets are stored on HDFS in the `/user/pnda/PNDA_datasets/datasets` directory in the [Kite](http://kitesdk.org/) format, and are partitioned by source and timestamp.

The current partition strategy will write the data read from Kafka to the following hierarchy:

```
source=SSS/year=YYYY/month=MM/day=DD/hour=HH
```

In the path above, `SSS` represents the value of the `src` field in the PNDA Avro schema.

For example, the following message read from Kafka:

    {
      "timestamp": 1462886335,
      "src": "netflow",
      "host_ip": "192.168.0.15",
      "rawdata": "XXXXXxxxxXXXX"
    }
    
will be written to:

```
/user/pnda/PNDA_datasets/datasets/source=netflow/year=2016/month=05/day=10/hour=13/random-uuid.avro
```

Please refer to the [Gobblin](https://github.com/linkedin/gobblin) for more information about Gobblin.

