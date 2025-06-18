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

import com.adobe.spark.sql.avro.client.RegistryClientFactory
import com.adobe.spark.sql.avro.config.AvroSerConfig
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, Row, SparkSession, functions}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import java.io.ByteArrayOutputStream
import java.util.UUID
import scala.collection.JavaConverters._

class CatalystToAvroJsonSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with DatasetComparer {

  private val MAGIC_BYTE = Array[Byte](0, 0, 0, 0)

  private val spark: SparkSession = SparkSession
    .builder()
    .appName("AvroBinaryToCatalystSpec")
    .master("local")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def beforeEach() {
    com.adobe.spark.sql.avro.functions.registerFunctions()
  }

  it should "serialize strings correctly" in {
    import spark.implicits._
    val schema = new Schema.Parser().parse("""{"type": "string"}""");
    val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
    val schemaManager = RegistryClientFactory.create(registryConfig)
    val schemaId = schemaManager.register("serialize-string-correctly", schema)
    val config = AvroSerConfig(schemaId, schema)
    val result = spark.createDataFrame(Seq(Row("key")).asJava, StructType(Seq(StructField("key", StringType, nullable = false))))
      .select(new Column(CatalystToAvroJson(functions.col("key").expr, config, registryConfig)).as("key"))
    val data = Seq(Row(asString(schema, "key")))
    val expected = spark.createDataFrame(data.asJava, StructType(Seq(StructField("key", StringType, nullable = false))))
    assertSmallDatasetEquality(result, expected)
  }

  it should "serialize struct correctly" in {
    import spark.implicits._
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
    val schemaManager = RegistryClientFactory.create(registryConfig)
    val schemaId = schemaManager.register("serialize-struct-correctly", schema)
    val config = AvroSerConfig(schemaId, schema)
    val result = spark.createDataFrame(
      Seq(Row(Row("k1", 1))).asJava,
      StructType.fromDDL("record struct<key STRING NOT NULL, value INTEGER NOT NULL> NOT NULL")
    ).select(new Column(CatalystToAvroJson(functions.col("record").expr, config, registryConfig)).as("record"))
    val data = Seq(
      Row(asString(schema, Map("key" -> "k1", "value" -> 1)))
    )
    val expected = spark.createDataFrame(data.asJava, StructType(Seq(StructField("record", StringType, nullable = false))))
    assertSmallDatasetEquality(result, expected)
  }

  it should "serialize struct nullable" in {
    import spark.implicits._
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
    val schemaManager = RegistryClientFactory.create(registryConfig)
    val schemaId = schemaManager.register("serialize-struct-nullable", schema)
    val config = AvroSerConfig(schemaId, schema)
    val result = spark.createDataFrame(
      Seq(Row(null)).asJava,
      StructType.fromDDL("record struct<key STRING NOT NULL, value INTEGER NOT NULL>")
    ).select(new Column(CatalystToAvroJson(functions.col("record").expr, config, registryConfig)).as("record"))
    val data = Seq(Row(null))
    val expected = spark.createDataFrame(data.asJava, StructType(Seq(StructField("record", StringType, nullable = true))))
    assertSmallDatasetEquality(result, expected)
  }

  it should "serialize struct only components nullable" in {
    import spark.implicits._
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
    val schemaManager = RegistryClientFactory.create(registryConfig)
    val schemaId = schemaManager.register("serialize-struct-components-nullable", schema)
    val config = AvroSerConfig(schemaId, schema)
    val result = spark.createDataFrame(
      Seq(Row(Row("k1", 1))).asJava,
      StructType.fromDDL("record struct<key STRING, value INTEGER> NOT NULL")
    ).select(new Column(CatalystToAvroJson(functions.col("record").expr, config, registryConfig)).as("record"))
    val data = Seq(Row(asString(schema, Map("key" -> "k1", "value" -> 1))))
    val expected = spark.createDataFrame(data.asJava, StructType(Seq(StructField("record", StringType, nullable = false))))
    assertSmallDatasetEquality(result, expected)
  }

  it should "work as sql expression" in {
    import spark.implicits._
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
    val schemaManager = RegistryClientFactory.create(registryConfig)
    val schemaId = schemaManager.register("serialize-struct-correctly", schema)
    val config = AvroSerConfig(schemaId, schema)
    val expected = spark.createDataFrame(
      Seq(Row(Row("k1", 1))).asJava,
      StructType.fromDDL("record struct<key STRING NOT NULL, value INTEGER NOT NULL> NOT NULL")
    ).select(new Column(CatalystToAvroJson(functions.col("record").expr, config, registryConfig)).as("record"))
    val result = spark.createDataFrame(
      Seq(Row(Row("k1", 1))).asJava,
      StructType.fromDDL("record struct<key STRING NOT NULL, value INTEGER NOT NULL> NOT NULL")
    )
      .select(expr(
        s"""
           |to_avro_json(
           | record, 
           | map(
           |   'subject', 'serialize-struct-correctly', 
           |   'writeSchemaId', 'true', 
           |   'magicByteSize', '4'
           | ), 
           | 'HELLO', 
           | map('schema.registry.url', 'mock://registry', 'max.schemas.per.subject', '200')
           |)
           |""".stripMargin).as("record"))
    assertSmallDatasetEquality(result, expected)
  }

  it should "work as sql query" in {
    import spark.implicits._
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
    val schemaManager = RegistryClientFactory.create(registryConfig)
    val schemaId = schemaManager.register("serialize-struct-correctly", schema)
    val config = AvroSerConfig(schemaId, schema)
    val expected = spark.createDataFrame(
      Seq(Row(Row("k1", 1))).asJava,
      StructType.fromDDL("record struct<key STRING NOT NULL, value INTEGER NOT NULL> NOT NULL")
    ).select(new Column(CatalystToAvroJson(functions.col("record").expr, config, registryConfig)).as("record"))
    val randomViewId = UUID.randomUUID.toString.replace("-", "")
    spark.createDataFrame(
      Seq(Row(Row("k1", 1))).asJava,
      StructType.fromDDL("record struct<key STRING NOT NULL, value INTEGER NOT NULL> NOT NULL")
    ).createTempView(randomViewId)
    val result = spark.sql(
        s"""
           |SELECT 
           |  to_avro_json(
           |   record, 
           |   map(
           |     'subject', 'serialize-struct-correctly', 
           |     'writeSchemaId', 'true', 
           |     'magicByteSize', '4'
           |   ), 
           |   'HELLO', 
           |   map('schema.registry.url', 'mock://registry', 'max.schemas.per.subject', '200')
           |  ) AS record
           |FROM `${randomViewId}`
           |""".stripMargin
        )
    assertSmallDatasetEquality(result, expected)
  }

  private def asBytes(schema: Schema, datum: Any): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().jsonEncoder(schema, out)
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

  private def asString(schema: Schema, datum: Any): String = {
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().jsonEncoder(schema, out)
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
    out.toString
  }

}
