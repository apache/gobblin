Table of Contents
--------------------

[TOC]

## Source schema
A source schema has to be declared before extracting the data from the source. 
To define the source schema `source.schema` property is available which takes a JSON value defining the source schema. 
This schema is used by Converters to perform data type or data format conversions. 
The java class representation of a source schema can be found here [Schema.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/source/extractor/schema/Schema.java).

## Converters
In Gobblin library a Converter is an interface for classes that implement data transformations, e.g., data type conversions,
schema projections, data manipulations, data filtering, etc. This interface is responsible for 
converting both schema and data records. Classes implementing this interface are composible and 
can be chained together to achieve more complex data transformations.

A converter basically needs four inputs:
- Input schema
- Output schema type
- Input data
- Output data type

There are various inbuilt Converters available within gobblin-core. However, you can also implement your own converter 
by extending abstract class ```org.apache.gobblin.converter.Converter```. Below, is example of such a custom implementation 
of Gobblin Converter which replaces multiple newlines and spaces from JSON values.

``` java

package org.apache.gobblin.example.sample;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class FilterSpacesConverter extends Converter<JsonArray, JsonArray, JsonObject, JsonObject> {
  @Override
  public JsonArray convertSchema(JsonArray inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema; //We are not doing any schema conversion
  }

  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, JsonObject inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    String jsonStr = inputRecord.toString().replaceAll("\\s{2,}", " ");
    return new SingleRecordIterable<>(new JsonParser().parse(jsonStr).getAsJsonObject());
  }
}
```
The converters can also be chained to perform sequential conversion on each input record. 
To chain converters use the property ```converter.classes``` and provide a list of comma separated 
converters with full reference name of converters. The execution order of the converters is same as 
defined in the comma separated list. 

For example:
If you are reading data from a JsonSource and you want to write data into Avro format. 
For this you can chain the converters to convert from Json string to Json and the convert Json into 
Avro. By using the following property in your .pull file.
```converter.classes="org.apache.gobblin.converter.json.JsonStringToJsonIntermediateConverter,org.apache.gobblin.converter.avro.JsonIntermediateToAvroConverter"```

## Converters available in Gobblin
- [AvroFieldRetrieverConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/avro/AvroFieldRetrieverConverter.java)
- [AvroRecordToAvroWritableConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/avro/AvroRecordToAvroWritableConverter.java)
- [AvroToAvroCopyableConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/avro/AvroToAvroCopyableConverter.java)
- [AvroToBytesConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/avro/AvroToBytesConverter.java)
- [BytesToAvroConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/avro/BytesToAvroConverter.java)
- [FlattenNestedKeyConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/avro/FlattenNestedKeyConverter.java)
- [JsonIntermediateToAvroConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/avro/JsonIntermediateToAvroConverter.java)
- [JsonRecordAvroSchemaToAvroConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/avro/JsonRecordAvroSchemaToAvroConverter.java)
- [CsvToJsonConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/csv/CsvToJsonConverter.java)
- [CsvToJsonConverterV2.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/csv/CsvToJsonConverterV2.java)
- [AvroFieldsPickConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/filter/AvroFieldsPickConverter.java)
- [AvroFilterConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/filter/AvroFilterConverter.java)
- [AvroToRestJsonEntryConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/http/AvroToRestJsonEntryConverter.java)
- [BytesToJsonConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/json/BytesToJsonConverter.java)
- [JsonStringToJsonIntermediateConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/json/JsonStringToJsonIntermediateConverter.java)
- [JsonToStringConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/json/JsonToStringConverter.java)
- [ObjectStoreConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/objectstore/ObjectStoreConverter.java)
- [ObjectStoreDeleteConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/objectstore/ObjectStoreDeleteConverter.java)
- [HiveSerDeConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/serde/HiveSerDeConverter.java)
- [ObjectToStringConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/string/ObjectToStringConverter.java)
- [StringFilterConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/string/StringFilterConverter.java)
- [StringSplitterConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/string/StringSplitterConverter.java)
- [StringSplitterToListConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/string/StringSplitterToListConverter.java)
- [StringToBytesConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/string/StringToBytesConverter.java)
- [TextToStringConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/string/TextToStringConverter.java)
- [GobblinMetricsPinotFlattenerConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/GobblinMetricsPinotFlattenerConverter.java)


## Schema specification
  The following section discusses the specification to define source schema using a JSON format.

| Key Name     			| Value data type   	| Description                                             |
|-----------------------|-----------------------|---------------------------------------------------------|
| columnName			| String            	| The name of the JSON key which will contain the data.   |
| isNullable			| Boolean				| Can data be null?                         |
| comment				| String				| Field description just for documentation purpose.|
| dataType				| JSON					| Provides more information about the data type.                   |
| dataType.type			| String				| Type of data to store. ex: int, long etc                |
| dataType.name			| String				| Provide a name to your data type.                       |
| dataType.items		| String/JSON			| Used for array type to define the data type of items contained by the array. If data type of array items is primitive the String is used as value otherwise for complex type dataType JSON should be used as a value to provide further information on complex array items.  |
| dataType.values		| String/JSON/Array		| Used by map and record types to define the data type of the values. In case of records it will always be Array type defining fields. In case of map it could be String or JSON based on primitive or complex data type involved.|
| dataype.symbols		| Array<String>			| Array of strings to define the enum symbols. |
| watermark				| Boolean				| To specify if the key is used as a watermark. Or use `extract.delta.fields` property to define comma separated list of watermark fields. |
| unique			| Boolean				| To specify if the key should be unique set of records. |
| defaultValue			| Object				| To specify the default value. |

## Supported data types by different converters
The converters which perform data format conversions such as CSV to JSON, JSON to AVRO etc. will have to perform data type conversions. Below, is the list of such converters and the data types they support.

| Converter  | Data types  |
|---|---|
| [JsonIntermediateToAvroConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/converter/avro/JsonIntermediateToAvroConverter.java)  | <ul><li>DATE</li><li>TIMESTAMP</li><li>TIME</li><li>STRING</li><li>BYTES</li><li>INT</li><li>LONG</li><li>FLOAT</li><li>DOUBLE</li><li>BOOLEAN</li><li>ARRAY</li><li>MAP</li><li>ENUM</li></ul>|
| [JsonIntermediateToParquetGroupConverter.java](https://github.com/apache/incubator-gobblin/blob/master/gobblin-modules/gobblin-parquet/src/main/java/org/apache/gobblin/converter/parquet/JsonIntermediateToParquetGroupConverter.java)  | <ul><li>DATE</li><li>TIMESTAMP</li><li>TIME</li><li>STRING</li><li>BYTES</li><li>INT</li><li>LONG</li><li>FLOAT</li><li>DOUBLE</li><li>BOOLEAN</li><li>ARRAY</li><li>MAP</li><li>ENUM</li></ul>|


### Primitive types 
 The following primitive types are available int, float, string, double, long, null, boolean.
 
**Sample data**

```js
{
	"jobRoles": 42,
	"peopleWeightAvg": 50.5,
	"peopleOrg": "EvilCorp",
	"peopleAvgSal": 342222.65,
        "peopleCount": 8344242342,
	"peopleBrain": null,
	"public": false
}
```
**Sample schema**
```js
[
    {
        "columnName": "jobRoles",
        "isNullable": false,
        "comment": "Number of roles in the org"
        "dataType": {
                "type": "int"
            }
    },
    {
        "columnName": "peopleWeightAvg",
        "isNullable": false,
        "comment": "Avg weight of people in org"
        "dataType": {
                "type": "float"
            }
    },
    {
        "columnName": "peopleOrg",
        "isNullable": false,
        "comment": "Name of org people works for"
        "dataType": {
                "type": "string"
            }
    },
    {
        "columnName": "peopleAvgSal",
        "isNullable": false,
        "comment": "Avg salary of people in org"
        "dataType": {
                "type": "double"
            }
    },
    {
        "columnName": "peopleCount",
        "isNullable": false,
        "comment": "Count of people in org"
        "dataType": {
                "type": "long"
            }
    },
    {
        "columnName": "peopleBrain",
        "comment": "Brain obj of people"
        "dataType": {
                "type": "null"
            }
    },
    {
        "columnName": "public",
        "isNullable": false,
        "comment": "Is data public"
        "dataType": {
                "type": "boolean"
            }
    }
]
```


### Complex types
#### Array

**Sample data**
```js
{
	"arrayOfInts": [25, 50, 75]
}
```
**Sample schema**
```js
[
    {
        "columnName": "arrayOfInts",
        "isNullable": false,
        "comment": "Items in array have same data type as defined in dataType."
        "dataType": {
                "type": "array",
                "items": "int"
            }
    }
]
```
#### Map
Maps can contain n number of key value pairs with constraint of same data type for values and keys are always string.
**Sample data**
```js
{
	"bookDetails":{
		"harry potter and the deathly hallows": 10245,
		"harry potter and the cursed child": 20362
	}
}
```

**Sample schema**

```js
[
    {
        "columnName": "bookDetails",
        "isNullable": false,
        "comment": "Maps always have string as keys and all values have same type as defined in dataType"
        "dataType": {
                "type": "map",
                "values": "long"
            }
    }
]
```

#### Record
Unlike map, values in record type are not bound by single value type. Keys and values have to be declared in the schema with data type.
**Sample data**
```js
{
	"userDetails": {
		"userName": "anonyoumous",
		"userAge": 50,
	}
}
```
**Sample schema**
```js
[
    {
        "columnName": "userDetails",
        "isNullable": false,
        "comment": "user detail"
        "dataType": {
                "type": "record",
                "values": [
                    {
                        "columnName": "userName",
                        "dataType":{
                            "type":"string"
                        }
                    },
                    {
                        "columnName": "userAge",
                        "dataType":{
                            "type":"int"
                        }
                    }
                ]
            }
    }
]
```

#### Enum
**Sample data**
```js
{
	"userStatus": "ACTIVE"
}
```
**Sample schema**
```js
[
    {
        "columnName": "userStatus",
        "dataType":{
            "type": "enum",
            "symbols":[
                "ACTIVE", "INACTIVE"
            ]
        }
    }
]
```

### Nesting types
Complex types can be used to created nested schemas.
**Array, Map and Record can have complex items instead of just primitive types.**

Few of the examples to show how nested schema is written

**Array with nested record**
```js
[
  {
    "columnName": "userName",
    "dataType": {
      "type": "string"
    }
  },
  {
    "columnName": "purchase",
    "dataType": {
      "type": "array",
      "items": {
        "dataType": {
          "type": "record",
          "values": [
            {
              "columnName": "ProductName",
              "dataType": {
                "type": "string"
              }
            },
            {
              "columnName": "ProductPrice",
              "dataType": {
                "type": "long"
              }
            }
          ]
        }
      }
    }
  }
]
```
**Map with nested array**
```js
[
  {
    "columnName": "persons",
    "dataType": {
      "type": "map",
      "values": {
        "dataType": {
          "type": "array",
          "items": "int"
        }
      }
    }
  }
]
```