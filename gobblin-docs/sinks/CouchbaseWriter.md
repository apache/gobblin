[TOC]

# Introduction
The [CouchbaseWriter](https://github.com/apache/incubator-gobblin/blob/master/gobblin-modules/gobblin-couchbase/src/main/java/org/apache/gobblin/couchbase/writer/CouchbaseWriter.java) supports writing documents to a Couchbase bucket though the [Couchbase Java SDK](https://docs.couchbase.com/java-sdk/current/start-using-sdk.html). Note that CouchbaseWriter only supports writing to a single bucket as there should be only 1 CouchbaseEnvironment per JVM.


# Record format
Couchbase writer currently support `AVRO` and `JSON` as data inputs. On both of them it requires the following structured schema:


| Document field | Description |
| -------------- | ----------- |
| `key` | Unique key used to store the document on the bucket. For more info view [Couchbase docs](https://developer.couchbase.com/documentation/server/3.x/developer/dev-guide-3.0/keys-values.html)  |
| `data.data` | Object or value containing the information associated with the `key` for this document |
| `data.flags` | [Couchbase flags](https://docs.couchbase.com/server/4.1/developer-guide/transcoders.html) To store JSON on `data.data` use `0x02 << 24` for UTF-8 `0x04 << 24` . |

The following is a sample input record with JSON data

```json
{
 "key": "myKey123",
 "data": {
    "data": {
        "field1": "field1Value",
        "field2": 123
    },
    "flags": 33554432
  }
}
```

or to store plain text:

```json
{
 "key": "myKey123",
 "data": {
    "data": "singleValueData",
    "flags": 67108864
  }
}
```

If using AVRO, use the following schema:

```json
{
  "type" : "record",
  "name" : "topLevelRecord",
  "fields" : [ {
    "name" : "key",
    "type" : "string"
  }, {
    "name" : "data",
    "type" : {
      "type" : "record",
      "name" : "data",
      "namespace" : "topLevelRecord",
      "fields" : [ {
        "name" : "data",
        "type" : [ "bytes", "null" ]
      }, {
        "name" : "flags",
        "type" : "int"
      } ]
    }
  } ]
}
```
Note that the key can be other than string if needed.
# Configuration
## General configuration values
| Configuration Key | Default Value | Description |
| ----------------- | ------------- | ----------- |
| `writer.couchbase.bucket` | Optional | Name of the couchbase bucket. Change if using other than default bucket |
| `writer.couchbase.default` | `"default"` | Name of the default bucket if `writer.couchbase.bucket` is not provided |
| `writer.couchbase.dnsSrvEnabled` | `"false"` | Enable DNS SRV bootstrapping [docs](https://docs.couchbase.com/java-sdk/current/managing-connections.html) | 
| `writer.couchbase.bootstrapServers | `localhost` | URL to bootstrap servers. If using DNS SRV set `writer.couchbase.dnsSrvEnabled` to true |
| `writer.couchbase.sslEnabled` | `false` | Use SSL to connect to couchbase |
| `writer.couchbase.password` | Optional | Bucket password. Will be ignored if `writer.couchbase.certAuthEnabled` is true |
| `writer.couchbase.certAuthEnabled` | `false` | Set to true if using certificate authentication. Must also specify `writer.couchbase.sslKeystoreFile`, `writer.couchbase.sslKeystorePassword`, `writer.couchbase.sslTruststoreFile`, and `writer.couchbase.sslTruststorePassword` |
| `writer.couchbase.sslKeystoreFile` | Optional | Path to the keystore file location |
| `writer.couchbase.sslKeystorePassword` | Optional | Keystore password |
| `writer.couchbase.sslTruststoreFile` | Optional | Path to the trustStore file location |
| `writer.couchbase.sslTruststorePassword` | Optional | TrustStore password |
| `writer.couchbase.documentTTL` | `0` | Time To Live of each document. Units are specified in `writer.couchbase.documentTTLOriginField` |
| `writer.couchbase.documentTTLUnits` | `SECONDS` | Unit for `writer.couchbase.documentTTL`. Must be one of [java.util.concurrent.TimeUnit](https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/TimeUnit.html). Case insensitive  |
| `writer.couchbase.documentTTLOriginField` | Optional | Time To Live of each document. Units are specified in `writer.couchbase.documentTTLOriginField` |
| `writer.couchbase.documentTTLOriginUnits` | `MILLISECONDS` | Unit for `writer.couchbase.documentTTL`. Must be one of [java.util.concurrent.TimeUnit](https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/TimeUnit.html). Case insensitive. As an example a `writer.couchbase.documentTTLOriginField` value of `1568240399000` and `writer.couchbase.documentTTLOriginUnits` value of `MILLISECONDS` timeunit would be `Wed Sep 11 15:19:59 PDT 2019` |
| `writer.couchbase.retriesEnabled` | `false` | Enable write retries on failures |
| `writer.couchbase.maxRetries` | `5` | Maximum number of retries |
| `writer.couchbase.failureAllowancePercentage` | `0.0` | The percentage of failures that you are willing to tolerate while writing to Couchbase. Gobblin will mark the workunit successful and move on if there are failures but not enough to trip the failure threshold. Only successfully acknowledged writes are counted as successful, all others are considered as failures. The default for the failureAllowancePercentage is set to 0.0. For example, if the value is set to 0.2 This means that as long as 80% of the data is acknowledged by Couchbase, Gobblin will move on. If you want higher guarantees, set this config value to a lower value. e.g. If you want 99% delivery guarantees, set this value to 0.01 |
|`operationTimeoutMillis` | `10000` | Global timeout for couchbase communication operations |

## Authentication
### No credentials
NOT RECOMMENDED FOR PRODUCTION.

Do not set `writer.couchbase.certAuthEnabled` nor `writer.couchbase.password`
### Using certificates
Set `writer.couchbase.certAuthEnabled` to `true` and values for `writer.couchbase.sslKeystoreFile`, `writer.couchbase.sslKeystorePassword`, `writer.couchbase.sslTruststoreFile`, and `writer.couchbase.sslTruststorePassword`.

`writer.couchbase.password` setting will be ignored if `writer.couchbase.certAuthEnabled` is set
### Using bucket password
Set `writer.couchbase.password`

## Document level expiration
Couchbase writer allows to set expiration at the document level using the [expiry](https://docs.couchbase.com/java-sdk/current/document-operations.html) property of the couchbase document. PLease note that current couchbase implementation using timestamps limits it to January 19, 2038 03:14:07 GM given the type of expiry is set to int. CouchbaseWriter only works with global timestamps and does not use relative expiration in seconds (<30 days) for simplicity.
Currently three modes are supported:
### 1 - Expiration from write time
Define only `writer.couchbase.documentTTL` and `writer.couchbase.documentTTLUnits`. For example for a 2 days expiration configs would look like:

| Configuration Key | Value |
| ----------------- | ------------- |
| `writer.couchbase.documentTTL` | `2` |
| `writer.couchbase.documentTTLUnits` | `DAYS` |

### 2 - Expiration from an origin timestamp
Define only `writer.couchbase.documentTTL` and `writer.couchbase.documentTTLUnits`.

For example for a 2 days expiration configs using the `header.time` field that has timestamp in MILLISECONDS would look like:

| Configuration Key | Value |
| ----------------- | ------------- |
| `writer.couchbase.documentTTL` | `2` |
| `writer.couchbase.documentTTLUnits` | `"DAYS"` |
| `writer.couchbase.documentTTLOriginField` | `"header.time"` |
| `writer.couchbase.documentTTLOriginUnits` | `1568240399000` |

So a sample document with origin on 1568240399 (Wed Sep 11 15:19:59 PDT 2019) would expire on 1568413199 (Fri Sep 13 15:19:59 PDT 2019). The following is a sample record format.

```json
{
 "key": "sampleKey",
 "data": {
    "data": {
        "field1": "field1Value",
        "header": {
            "time": 1568240399000
        }
    },
    "flags": 33554432
  }
}
```

}
