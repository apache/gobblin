/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.multistage.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import org.apache.gobblin.multistage.util.JsonElementTypes;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.gobblin.multistage.util.JsonElementTypes.*;


/**
 * Test following methods of JsonElementTypes
 *
 * test 1: map element type to an avro alternative type
 * test 2: primitive flag
 * test 3: array flag, object flag, and null flag
 * test 4: reverse nullability
 * test 5: infer type from values
 * test 6: infer type from a union
 */
@Test
public class JsonElementTypesTest {
    /**
     * Test element type to avro alternative type mapping
     */
    @Test
    public void testAltName() {
        Assert.assertEquals(JsonElementTypes.ARRAY.getAltName(), "array");
        Assert.assertEquals(JsonElementTypes.BOOLEAN.getAltName(), "boolean");
        Assert.assertEquals(JsonElementTypes.DATE.getAltName(), "date");
        Assert.assertEquals(JsonElementTypes.DOUBLE.getAltName(), "double");
        Assert.assertEquals(JsonElementTypes.ENUM.getAltName(), "enum");
        Assert.assertEquals(JsonElementTypes.FLOAT.getAltName(), "double");
        Assert.assertEquals(JsonElementTypes.INT.getAltName(), "int");
        Assert.assertEquals(JsonElementTypes.INTEGER.getAltName(), "int");
        Assert.assertEquals(JsonElementTypes.INT64.getAltName(), "long");
        Assert.assertEquals(JsonElementTypes.LONG.getAltName(), "long");
        Assert.assertEquals(JsonElementTypes.NULL.getAltName(), "null");
        Assert.assertEquals(JsonElementTypes.NUMBER.getAltName(), "double");
        Assert.assertEquals(JsonElementTypes.OBJECT.getAltName(), "record");
        Assert.assertEquals(JsonElementTypes.PRIMITIVE.getAltName(), "primitive");
        Assert.assertEquals(JsonElementTypes.RECORD.getAltName(), "record");
        Assert.assertEquals(JsonElementTypes.STRING.getAltName(), "string");
        Assert.assertEquals(JsonElementTypes.TIME.getAltName(), "time");
        Assert.assertEquals(JsonElementTypes.TIMESTAMP.getAltName(), "timestamp");
        Assert.assertEquals(JsonElementTypes.UNION.getAltName(), "union");
        Assert.assertEquals(JsonElementTypes.UNKNOWN.getAltName(), "unknown");
        Assert.assertEquals(JsonElementTypes.NULLABLEARRAY.getAltName(), "array");
        Assert.assertEquals(JsonElementTypes.NULLABLEBOOLEAN.getAltName(), "boolean");
        Assert.assertEquals(JsonElementTypes.NULLABLEDOUBLE.getAltName(), "double");
        Assert.assertEquals(JsonElementTypes.NULLABLEINT.getAltName(), "int");
        Assert.assertEquals(JsonElementTypes.NULLABLELONG.getAltName(), "long");
        Assert.assertEquals(JsonElementTypes.NULLABLEOBJECT.getAltName(), "record");
        Assert.assertEquals(JsonElementTypes.NULLABLERECORD.getAltName(), "record");
        Assert.assertEquals(NULLABLESTRING.getAltName(), "string");
        Assert.assertEquals(JsonElementTypes.NULLABLETIME.getAltName(), "time");
        Assert.assertEquals(JsonElementTypes.NULLABLETIMESTAMP.getAltName(), "timestamp");
    }

    /**
     * Test primitive flag of each element
     */
    @Test
    public void testPrimitiveFlag() {
        Assert.assertFalse(JsonElementTypes.ARRAY.isPrimitive());
        Assert.assertTrue(JsonElementTypes.BOOLEAN.isPrimitive());
        Assert.assertTrue(JsonElementTypes.DATE.isPrimitive());
        Assert.assertTrue(JsonElementTypes.DOUBLE.isPrimitive());
        Assert.assertTrue(JsonElementTypes.ENUM.isPrimitive());
        Assert.assertTrue(JsonElementTypes.FLOAT.isPrimitive());
        Assert.assertTrue(JsonElementTypes.INT.isPrimitive());
        Assert.assertTrue(JsonElementTypes.INTEGER.isPrimitive());
        Assert.assertTrue(JsonElementTypes.INT64.isPrimitive());
        Assert.assertTrue(JsonElementTypes.LONG.isPrimitive());
        Assert.assertTrue(JsonElementTypes.NULL.isPrimitive());
        Assert.assertTrue(JsonElementTypes.NUMBER.isPrimitive());
        Assert.assertFalse(JsonElementTypes.OBJECT.isPrimitive());
        Assert.assertTrue(JsonElementTypes.PRIMITIVE.isPrimitive());
        Assert.assertFalse(JsonElementTypes.RECORD.isPrimitive());
        Assert.assertTrue(JsonElementTypes.STRING.isPrimitive());
        Assert.assertTrue(JsonElementTypes.TIME.isPrimitive());
        Assert.assertTrue(JsonElementTypes.TIMESTAMP.isPrimitive());
        Assert.assertFalse(JsonElementTypes.UNION.isPrimitive());
        Assert.assertTrue(JsonElementTypes.UNKNOWN.isPrimitive());
        Assert.assertFalse(JsonElementTypes.NULLABLEARRAY.isPrimitive());
        Assert.assertTrue(JsonElementTypes.NULLABLEBOOLEAN.isPrimitive());
        Assert.assertTrue(JsonElementTypes.NULLABLEDOUBLE.isPrimitive());
        Assert.assertTrue(JsonElementTypes.NULLABLEINT.isPrimitive());
        Assert.assertTrue(JsonElementTypes.NULLABLELONG.isPrimitive());
        Assert.assertFalse(JsonElementTypes.NULLABLEOBJECT.isPrimitive());
        Assert.assertFalse(JsonElementTypes.NULLABLERECORD.isPrimitive());
        Assert.assertTrue(NULLABLESTRING.isPrimitive());
        Assert.assertTrue(JsonElementTypes.NULLABLETIME.isPrimitive());
        Assert.assertTrue(JsonElementTypes.NULLABLETIMESTAMP.isPrimitive());
    }

    /**
     * check array flag
     * Input : JsonElementTypes.ARRAY or NULLABLEARRAY
     * Output: true
     *
     * Input : anything else
     */
    public void testArrayFlag() {
        Assert.assertTrue(JsonElementTypes.ARRAY.isArray());
        Assert.assertTrue(JsonElementTypes.NULLABLEARRAY.isArray());

        Assert.assertFalse(JsonElementTypes.BOOLEAN.isArray());
        Assert.assertFalse(JsonElementTypes.INT.isArray());
        Assert.assertFalse(JsonElementTypes.OBJECT.isArray());
        Assert.assertFalse(JsonElementTypes.STRING.isArray());
        Assert.assertFalse(JsonElementTypes.NULLABLEBOOLEAN.isArray());
        Assert.assertFalse(JsonElementTypes.NULLABLEINT.isArray());
        Assert.assertFalse(NULLABLESTRING.isArray());
    }

    /**
     * check object flag
     * Input : JsonElementTypes.OBJECT or NULLABLEOBJECT
     * Output: true
     *
     * Input : anything else
     */
    public void testObjectFlag() {
        Assert.assertTrue(JsonElementTypes.OBJECT.isObject());
        Assert.assertTrue(JsonElementTypes.NULLABLEOBJECT.isObject());

        Assert.assertFalse(JsonElementTypes.ARRAY.isObject());
        Assert.assertFalse(JsonElementTypes.BOOLEAN.isObject());
        Assert.assertFalse(JsonElementTypes.INT.isObject());
        Assert.assertFalse(JsonElementTypes.STRING.isObject());
        Assert.assertFalse(JsonElementTypes.NULLABLEARRAY.isObject());
        Assert.assertFalse(JsonElementTypes.NULLABLEBOOLEAN.isObject());
        Assert.assertFalse(JsonElementTypes.NULLABLEINT.isObject());
        Assert.assertFalse(NULLABLESTRING.isObject());
    }

    /**
     * check NULL flag
     * Input : JsonElementTypes.NULL
     * Output: true
     *
     * Input : anything else
     * Output: false
     */
    public void testNullFlag() {
        Assert.assertTrue(JsonElementTypes.NULL.isNull());

        Assert.assertFalse(JsonElementTypes.ARRAY.isNull());
        Assert.assertFalse(JsonElementTypes.BOOLEAN.isNull());
        Assert.assertFalse(JsonElementTypes.INT.isNull());
        Assert.assertFalse(JsonElementTypes.OBJECT.isNull());
        Assert.assertFalse(JsonElementTypes.STRING.isNull());
        Assert.assertFalse(JsonElementTypes.NULLABLEARRAY.isNull());
        Assert.assertFalse(JsonElementTypes.NULLABLEBOOLEAN.isNull());
        Assert.assertFalse(JsonElementTypes.NULLABLEINT.isNull());
        Assert.assertFalse(JsonElementTypes.NULLABLEOBJECT.isNull());
        Assert.assertFalse(NULLABLESTRING.isNull());
    }

    /**
     * test reverse nullability
     */
    public void testReverseNullability() {
        Assert.assertEquals(JsonElementTypes.ARRAY.reverseNullability(), JsonElementTypes.NULLABLEARRAY);
        Assert.assertEquals(JsonElementTypes.BOOLEAN.reverseNullability(), JsonElementTypes.NULLABLEBOOLEAN);
        Assert.assertEquals(JsonElementTypes.INT.reverseNullability(), JsonElementTypes.NULLABLEINT);
        Assert.assertEquals(JsonElementTypes.OBJECT.reverseNullability(), JsonElementTypes.NULLABLEOBJECT);
        Assert.assertEquals(JsonElementTypes.RECORD.reverseNullability(), JsonElementTypes.NULLABLERECORD);
        Assert.assertEquals(JsonElementTypes.STRING.reverseNullability(), NULLABLESTRING);
        Assert.assertEquals(JsonElementTypes.NULLABLEARRAY.reverseNullability(), JsonElementTypes.ARRAY);
        Assert.assertEquals(JsonElementTypes.NULLABLEBOOLEAN.reverseNullability(), JsonElementTypes.BOOLEAN);
        Assert.assertEquals(JsonElementTypes.NULLABLEINT.reverseNullability(), JsonElementTypes.INT);
        Assert.assertEquals(JsonElementTypes.NULLABLEOBJECT.reverseNullability(), JsonElementTypes.OBJECT);
        Assert.assertEquals(JsonElementTypes.NULLABLERECORD.reverseNullability(), JsonElementTypes.RECORD);
        Assert.assertEquals(NULLABLESTRING.reverseNullability(), JsonElementTypes.STRING);
    }

    /**
     * Test getTypeFromMultiple()
     */
    public void testGetTypeFromMultiple() {
        JsonArray test1 = new Gson().fromJson("[]", JsonArray.class);
        Assert.assertEquals(JsonElementTypes.getTypeFromMultiple(test1), JsonElementTypes.NULL);

        JsonArray test2 = new Gson().fromJson("[10, 100]", JsonArray.class);
        Assert.assertEquals(JsonElementTypes.getTypeFromMultiple(test2), JsonElementTypes.INT);

        JsonArray test3 = new Gson().fromJson("[10, 100, null]", JsonArray.class);
        Assert.assertEquals(JsonElementTypes.getTypeFromMultiple(test3), JsonElementTypes.NULLABLEINT);

        JsonArray test4 = new Gson().fromJson("[{\"value\": 10}, {\"value\": 100}]", JsonArray.class);
        Assert.assertEquals(JsonElementTypes.getTypeFromMultiple(test4), JsonElementTypes.OBJECT);

        JsonArray test5 = new Gson().fromJson("[null, {\"value\": 10}, {\"value\": 100}]", JsonArray.class);
        Assert.assertEquals(JsonElementTypes.getTypeFromMultiple(test5), JsonElementTypes.NULLABLEOBJECT);

        JsonArray test6 = new Gson().fromJson("[\"test.string\"]", JsonArray.class);
        Assert.assertEquals(JsonElementTypes.getTypeFromMultiple(test6), STRING);

        JsonArray test7 = new Gson().fromJson("[null,\"test.string\"]", JsonArray.class);
        Assert.assertEquals(JsonElementTypes.getTypeFromMultiple(test7), NULLABLESTRING);

        JsonArray test8 = new Gson().fromJson("[true]", JsonArray.class);
        Assert.assertEquals(JsonElementTypes.getTypeFromMultiple(test8), BOOLEAN);

        JsonArray test9 = new Gson().fromJson("[true,null,false]", JsonArray.class);
        Assert.assertEquals(JsonElementTypes.getTypeFromMultiple(test9), NULLABLEBOOLEAN);

        JsonArray jsonArray = new JsonArray();
        jsonArray.add(JsonNull.INSTANCE);
        jsonArray.add(-7223372036854775808L);
        Assert.assertEquals(JsonElementTypes.getTypeFromMultiple(jsonArray), NULLABLELONG);

        JsonArray test11 = new Gson().fromJson("[null,123.23]", JsonArray.class);
        Assert.assertEquals(JsonElementTypes.getTypeFromMultiple(test11), NULLABLEDOUBLE);

        JsonArray test12 = new Gson().fromJson("[123.23]", JsonArray.class);
        Assert.assertEquals(JsonElementTypes.getTypeFromMultiple(test12), DOUBLE);

        JsonArray test13 = new Gson().fromJson("[null, 10]", JsonArray.class);
        Assert.assertEquals(JsonElementTypes.getTypeFromMultiple(test13), JsonElementTypes.NULLABLEINT);
    }

    /**
     * Test forType() converting a UNION string to JsonElementType
     * Input : "boolean"
     * Output: JsonElementTypes.BOOLEAN
     *
     * Input : "string"
     * Output: JsonElementTypes.STRING
     *
     * Input : ["string", "null"]
     * Output: JsonElementTypes.NULLABLESTRING
     */
    public void testForType() {
        String test1 = "boolean";
        Assert.assertEquals(JsonElementTypes.forType(test1), JsonElementTypes.BOOLEAN);

        String test2 = "string";
        Assert.assertEquals(JsonElementTypes.forType(test2), JsonElementTypes.STRING);

        String test3 = "[\"string\", \"null\"]";
        Assert.assertEquals(JsonElementTypes.forType(test3), NULLABLESTRING);

        String test4 = "[\"string\", \"primitive\", \"integer\"]";
        Assert.assertEquals(JsonElementTypes.forType(test4), UNION);
    }
}
