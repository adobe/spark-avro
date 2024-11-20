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

package com.adobe.spark.sql.avro.catalyst

import com.adobe.spark.sql.avro.client.RegistryFactory
import com.adobe.spark.sql.avro.config.AvroDeSerConfig
import com.adobe.spark.sql.avro.errors._
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.sql.{Column, Row, SparkSession, functions}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import java.io.ByteArrayOutputStream
import scala.collection.JavaConverters._

class AvroBinaryToCatalystSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with DatasetComparer {

  private val MAGIC_BYTE = Array[Byte](0, 0, 0, 0)

  private lazy val spark = SparkSession
    .builder()
    .appName("AvroBinaryToCatalystSpec")
    .master("local")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  it should "deserialize strings correctly" in {
    import spark.implicits._
    val schema = new Schema.Parser().parse("""{"type": "string"}""");
    val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
    val schemaManager = RegistryFactory.create(registryConfig)
    val schemaId = schemaManager.register("deserialize-string-correctly", new AvroSchema(schema))
    val config = AvroDeSerConfig(schemaId, schema, errOnEvolution = false, FailFastExceptionHandler())
    val data = Seq(Row(MAGIC_BYTE ++ BigInt(schemaId).toByteArray ++ asBytes(schema, "key")))
    val result = spark.createDataFrame(data.asJava, StructType(Seq(StructField("key", BinaryType))))
      .select(new Column(AvroBinaryToCatalyst(functions.col("key").expr, config, registryConfig)).as("key"))
    assertSmallDatasetEquality(result, Seq("key").toDF("key"))
  }

  it should "deserialize struct correctly" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "name": "value",
        |  "type": "record",
        |  "fields": [
        |    {"name": "key", "type": "string"},
        |    {"name": "value", "type": "int"}
        |  ]
        |}""".stripMargin
    );
    val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
    val schemaManager = RegistryFactory.create(registryConfig)
    val schemaId = schemaManager.register("deserialize-struct-correctly", new AvroSchema(schema))
    val config = AvroDeSerConfig(schemaId, schema, errOnEvolution = false, FailFastExceptionHandler())
    val data = Seq(Row(MAGIC_BYTE ++ BigInt(schemaId).toByteArray ++ asBytes(schema, Map("key" -> "k1", "value" -> 1))))
    val result = spark.createDataFrame(data.asJava, StructType(Seq(StructField("record", BinaryType))))
      .select(new Column(AvroBinaryToCatalyst(functions.col("record").expr, config, registryConfig)).as("record"))
    val expected = spark.createDataFrame(
      Seq(Row(Row("k1", 1))).asJava,
      StructType.fromDDL("record struct<key STRING NOT NULL, value INTEGER NOT NULL>")
    )
    assertSmallDatasetEquality(result, expected)
  }

  it should "deserialize struct permissively" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "name": "value",
        |  "type": "record",
        |  "fields": [
        |    {"name": "key", "type": ["string", "null"]},
        |    {"name": "value", "type": ["int", "null"]}
        |  ]
        |}""".stripMargin
    );
    val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
    val schemaManager = RegistryFactory.create(registryConfig)
    val schemaId = schemaManager.register("deserialize-struct-permissively", new AvroSchema(schema))
    val config = AvroDeSerConfig(schemaId, schema, errOnEvolution = false, PermissiveRecordExceptionHandler())
    val data = Seq(Row(MAGIC_BYTE ++ BigInt(schemaId).toByteArray))
    val result = spark.createDataFrame(data.asJava, StructType(Seq(StructField("record", BinaryType))))
      .select(new Column(AvroBinaryToCatalyst(functions.col("record").expr, config, registryConfig)).as("record"))
    val expected = spark.createDataFrame(
      Seq(Row(Row(null, null))).asJava,
      StructType.fromDDL("record struct<key STRING, value INTEGER>")
    )
    assertSmallDatasetEquality(result, expected)
  }

  it should "deserialize struct null if failed" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "name": "value",
        |  "type": "record",
        |  "fields": [
        |    {"name": "key", "type": "string"},
        |    {"name": "value", "type": "int"}
        |  ]
        |}""".stripMargin
    );
    val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
    val schemaManager = RegistryFactory.create(registryConfig)
    val schemaId = schemaManager.register("deserialize-struct-null", new AvroSchema(schema))
    val config = AvroDeSerConfig(schemaId, schema, errOnEvolution = false, NullReturningRecordExceptionHandler())
    val data = Seq(Row(MAGIC_BYTE ++ BigInt(schemaId).toByteArray))
    val result = spark.createDataFrame(data.asJava, StructType(Seq(StructField("record", BinaryType))))
      .select(new Column(AvroBinaryToCatalyst(functions.col("record").expr, config, registryConfig)).as("record"))
    val expected = spark.createDataFrame(
      Seq(Row(null)).asJava,
      StructType.fromDDL("record struct<key STRING NOT NULL, value INTEGER NOT NULL>")
    )
    assertSmallDatasetEquality(result, expected)
  }

  it should "deserialize with default" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "name": "value",
        |  "type": "record",
        |  "fields": [
        |    {"name": "key", "type": "string"},
        |    {"name": "value", "type": "int"}
        |  ]
        |}""".stripMargin
    );
    val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
    val schemaManager = RegistryFactory.create(registryConfig)
    val schemaId = schemaManager.register("deserialize-struct-default", new AvroSchema(schema))
    val default = new Record(schema);
    default.put("key", "defaultKey");
    default.put("value", -1);
    val config = AvroDeSerConfig(schemaId, schema, errOnEvolution = false, DeserializingDefaultRecordExceptionHandler(default))
    val data = Seq(Row(MAGIC_BYTE ++ BigInt(schemaId).toByteArray))
    val result = spark.createDataFrame(data.asJava, StructType(Seq(StructField("record", BinaryType))))
      .select(new Column(AvroBinaryToCatalyst(functions.col("record").expr, config, registryConfig)).as("record"))
    val expected = spark.createDataFrame(
      Seq(Row(Row("defaultKey", -1))).asJava,
      StructType.fromDDL("record struct<key STRING NOT NULL, value INTEGER NOT NULL>")
    )
    expected.show(false)
    println("||||||||")
    result.show(false)
    assertSmallDatasetEquality(result, expected)
  }

  it should "detect schema evolution" in {
    val schema = new Schema.Parser().parse("""{"type": "int"}""")
    val schemaV2 = new Schema.Parser().parse("""{"type": "string"}""")
    val registryConfig = Map("schema.registry.url" -> "mock://registry2", "max.schemas.per.subject" -> "200")
    val schemaManager = RegistryFactory.create(registryConfig)
    val schemaId = schemaManager.register("deserialize-string-correctly", new AvroSchema(schema))
    val schemaV2Id = schemaManager.register("deserialize-string-correctly", new AvroSchema(schemaV2))
    val config = AvroDeSerConfig(schemaId, schemaV2, errOnEvolution = true, NullReturningRecordExceptionHandler())
    val data = Seq(Row(MAGIC_BYTE ++ BigInt(schemaV2Id).toByteArray ++ asBytes(schemaV2, "key"))).asJava
    val result = spark.createDataFrame(data, StructType(Seq(StructField("key", BinaryType))))
      .select(new Column(AvroBinaryToCatalyst(functions.col("key").expr, config, registryConfig)).as("key"))
    the[Exception] thrownBy result.show() mustBe a[SchemaEvolutionError]
  }

  it should "work as sql expression" in {
    import spark.implicits._
    spark.sessionState.functionRegistry
      .registerFunction(FunctionIdentifier("from_avro_using_registry"),
        (children: Seq[Expression]) => new AvroBinaryToCatalyst(children.head, children(1), children(2), children.last),
        "built-in"
      )

    val strSchema = new Schema.Parser().parse("""{"type": "string"}""");
    val intSchema = new Schema.Parser().parse("""{"type": "int"}""");
    val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
    val schemaManager = RegistryFactory.create(registryConfig)
    val schemaId = schemaManager.register("deserialize-string-correctly", new AvroSchema(strSchema))
    val data = Seq(Row(MAGIC_BYTE ++ BigInt(schemaId).toByteArray ++ asBytes(intSchema, 1)))
    
    val result = spark.createDataFrame(data.asJava, StructType(Seq(StructField("key", BinaryType))))
      .select(expr(
        s"""
        |from_avro_using_registry(
        | key, 
        | map(
        |   'schemaId', '1', 
        |   'schema', '{"type": "string"}', 
        |   'errOnEvolution', 'false', 
        |   'errHandler', 'com.adobe.spark.sql.avro.errors.DefaultRecordExceptionHandler', 
        |   'magicByteSize', '4'
        | ), 
        | 'HELLO', 
        | map('schema.registry.url', 'mock://registry', 'max.schemas.per.subject', '200')
        |)
        |""".stripMargin).as("value"))

    val expected = Seq("HELLO").toDF("value")
    result.show(false)
    expected.show(false)
    assertSmallDatasetEquality(result, expected)
  }

  private def asBytes(schema: Schema, datum: Any): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    val writer = new GenericDatumWriter[Any](schema)
    datum match {
      case x: Map[String, Any] =>
        val record = new Record(schema);
        x.foreach { case (k, v) => record.put(k, v) }
        writer.write(record, encoder)
      case x: Map[Int, Any] =>
        val record = new Record(schema);
        x.foreach { case (k, v) => record.put(k, v) }
        writer.write(record, encoder)
      case x =>
        writer.write(x, encoder)
    }
    encoder.flush()
    out.toByteArray
  }

}

