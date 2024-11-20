/*
 * Copyright 2024 Adobe. All rights reserved.
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package com.adobe.spark.sql.avro.errors

import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{AvroRuntimeException, Schema}
import org.apache.spark.SparkException
import org.apache.spark.sql.avro.{DelegatingAvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DeSerExceptionHandlerTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  it should "FailFastExceptionHandler should fail fast" in {
    val schema = new Schema.Parser().parse("""{"type": "string"}""")
    val deserializer = new DelegatingAvroDeserializer(schema, SchemaConverters.toSqlType(schema).dataType)
    val handler = FailFastExceptionHandler()
    the[SparkException] thrownBy {
      handler.handle(new Exception(), deserializer, schema)
    } should have message "Malformed record detected. Fail fast."
  }

  it should "PermissiveRecordExceptionHandler should fail if schema is not a record" in {
    val schema = new Schema.Parser().parse("""{"type": "string"}""")
    val deserializer = new DelegatingAvroDeserializer(schema, SchemaConverters.toSqlType(schema).dataType)
    val handler = PermissiveRecordExceptionHandler()
    the[AvroRuntimeException] thrownBy {
      handler.handle(new Exception(), deserializer, schema)
    } should have message """Not a record schema: "string""""
  }

  it should "PermissiveRecordExceptionHandler should return empty record" in {
    val schema = new Schema.Parser().parse(
      """
        |{
        |   "type": "record", 
        |   "name": "rec", 
        |   "fields": [
        |       {"name": "f1", "type": ["null", "string"]}, 
        |       {"name": "f2", "type": ["null", "int"]}
        |   ]
        |}""".stripMargin)
    val deserializer = new DelegatingAvroDeserializer(schema, SchemaConverters.toSqlType(schema).dataType)
    val handler = PermissiveRecordExceptionHandler()
    val result = handler.handle(new Exception(), deserializer, schema).asInstanceOf[InternalRow]
    result.get(0, StringType) should equal(null);
    result.get(1, IntegerType) should equal(null)
  }

  it should "NullReturningRecordExceptionHandler should return null" in {
    val schema = new Schema.Parser().parse("""{"type": "string"}""")
    val deserializer = new DelegatingAvroDeserializer(schema, SchemaConverters.toSqlType(schema).dataType)
    val handler = NullReturningRecordExceptionHandler()
    val result = handler.handle(new Exception(), deserializer, schema).asInstanceOf[Object]
    result should equal(null)
  }

  it should "DefaultRecordExceptionHandler should return default value" in {
    val schema = new Schema.Parser().parse("""{"type": "string"}""")
    val deserializer = new DelegatingAvroDeserializer(schema, SchemaConverters.toSqlType(schema).dataType)
    val handler = DefaultRecordExceptionHandler("default")
    val result = handler.handle(new Exception(), deserializer, schema).asInstanceOf[Object]
    result should equal(handler.default)
  }

  it should "DeserializingDefaultRecordExceptionHandler should deserialize record and return default value" in {
    val schema = new Schema.Parser().parse(
      """
        |{
        |   "type": "record", 
        |   "name": "rec", 
        |   "fields": [
        |       {"name": "f1", "type": ["null", "int"]}
        |   ]
        |}""".stripMargin)
    val deserializer = new DelegatingAvroDeserializer(schema, SchemaConverters.toSqlType(schema).dataType)
    val default = new Record(schema);
    default.put("f1", -1);
    val handler = DeserializingDefaultRecordExceptionHandler(default)
    val result = handler.handle(new Exception(), deserializer, schema).asInstanceOf[InternalRow]
    result.get(0, IntegerType) should equal(-1)
  }


}
