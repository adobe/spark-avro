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
import com.adobe.spark.sql.avro.config.AvroDeSerConfig
import com.adobe.spark.sql.avro.errors._
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, Row, SparkSession, functions}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import java.io.ByteArrayOutputStream
import java.util.UUID
import scala.collection.JavaConverters._

class AvroJsonToCatalystSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with DatasetComparer {

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

  it should "deserialize strings correctly with id" in {
    import spark.implicits._
    val schema = new Schema.Parser().parse("""{"type": "string"}""");
    val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
    val schemaManager = RegistryClientFactory.create(registryConfig)
    val schemaId = schemaManager.register("deserialize-string-correctly-with-id", schema)
    val config = AvroDeSerConfig(schemaId, schema, errOnEvolution = false, FailFastExceptionHandler())
    val data = Seq(Row(asString(schema, "key")))
    val result = spark.createDataFrame(data.asJava, StructType(Seq(StructField("key", StringType))))
      .select(new Column(AvroJsonToCatalyst(functions.col("key").expr, functions.lit(schemaId.asInstanceOf[Int]).expr, config, registryConfig)).as("key"))
    assertSmallDatasetEquality(result, Seq("key").toDF("key"))
  }
  
  it should "deserialize struct correctly with id" in {
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
    val schemaId = schemaManager.register("deserialize-struct-correctly-with-id", schema)
    val config = AvroDeSerConfig(schemaId, schema, errOnEvolution = false, FailFastExceptionHandler())
    val data = Seq(Row(asString(schema, Map("key" -> "k1", "value" -> 1))))
    val result = spark.createDataFrame(data.asJava, StructType(Seq(StructField("record", StringType))))
      .select(new Column(AvroJsonToCatalyst(functions.col("record").expr, functions.lit(schemaId.asInstanceOf[Int]).expr, config, registryConfig)).as("record"))
    val expected = spark.createDataFrame(
      Seq(Row(Row("k1", 1))).asJava,
      StructType.fromDDL("record struct<key STRING NOT NULL, value INTEGER NOT NULL>")
    )
    assertSmallDatasetEquality(result, expected)
  }

  it should "deserialize struct permissively with id" in {
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
    val schemaId = schemaManager.register("deserialize-struct-permissively-with-id", schema)
    val config = AvroDeSerConfig(schemaId, schema, errOnEvolution = false, PermissiveRecordExceptionHandler())
    val data = Seq(Row(null.asInstanceOf[String]))
    val result = spark.createDataFrame(data.asJava, StructType(Seq(StructField("record", StringType))))
      .select(new Column(AvroJsonToCatalyst(functions.col("record").expr, functions.lit(schemaId.asInstanceOf[Int]).expr, config, registryConfig)).as("record"))
    val expected = spark.createDataFrame(
      Seq(Row(null)).asJava,
      StructType.fromDDL("record struct<key STRING, value INTEGER>")
    )
    assertSmallDatasetEquality(result, expected)
  }
  
  it should "deserialize struct null if failed with id" in {
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
    val schemaId = schemaManager.register("deserialize-struct-null-with-id", schema)
    val config = AvroDeSerConfig(schemaId, schema, errOnEvolution = false, NullReturningRecordExceptionHandler())
    val data = Seq(Row("not_a_json_object"))
    val result = spark.createDataFrame(data.asJava, StructType(Seq(StructField("record", StringType))))
      .select(new Column(AvroJsonToCatalyst(functions.col("record").expr, functions.lit(schemaId.asInstanceOf[Int]).expr, config, registryConfig)).as("record"))
    val expected = spark.createDataFrame(
      Seq(Row(null)).asJava,
      StructType.fromDDL("record struct<key STRING NOT NULL, value INTEGER NOT NULL>")
    )
    assertSmallDatasetEquality(result, expected)
  }
  
  it should "deserialize with default with id" in {
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
    val schemaId = schemaManager.register("deserialize-struct-default-with-id", schema)
    val default = new Record(schema);
    default.put("key", "defaultKey");
    default.put("value", -1);
    val config = AvroDeSerConfig(schemaId, schema, errOnEvolution = false, DeserializingDefaultRecordExceptionHandler(default))
    val data = Seq(Row("not_a_json_object"))
    val result = spark.createDataFrame(data.asJava, StructType(Seq(StructField("record", StringType))))
      .select(new Column(AvroJsonToCatalyst(functions.col("record").expr, functions.lit(schemaId.asInstanceOf[Int]).expr, config, registryConfig)).as("record"))
    val expected = spark.createDataFrame(
      Seq(Row(Row("defaultKey", -1))).asJava,
      StructType.fromDDL("record struct<key STRING NOT NULL, value INTEGER NOT NULL>")
    )
    result.collect()
    expected.show(false)
    println("||||||||")
    result.show(false)
    assertSmallDatasetEquality(result, expected)
  }

  it should "detect schema evolution with id" in {
    import spark.implicits._
    val schema = new Schema.Parser().parse("""{"type": "int"}""")
    val schemaV2 = new Schema.Parser().parse("""{"type": "string"}""")
    val registryConfig = Map("schema.registry.url" -> "mock://registry2", "max.schemas.per.subject" -> "200")
    val schemaManager = RegistryClientFactory.create(registryConfig)
    val schemaId = schemaManager.register("deserialize-string-correctly-with-id", schema)
    val schemaV2Id = schemaManager.register("deserialize-string-correctly-with-id", schemaV2)
    val config = AvroDeSerConfig(schemaId, schemaV2, errOnEvolution = true, NullReturningRecordExceptionHandler())
    val data = Seq(Row(asString(schemaV2, "key"))).asJava
    val result = spark.createDataFrame(data, StructType(Seq(StructField("key", StringType))))
      .select(new Column(AvroJsonToCatalyst(functions.col("key").expr, functions.lit(schemaV2Id.asInstanceOf[Int]).expr, config, registryConfig)).as("key"))
    the[Exception] thrownBy result.show() mustBe a[SchemaEvolutionError]
  }

  it should "work as sql expression with id" in {
    import spark.implicits._
    val strSchema = new Schema.Parser().parse("""{"type": "string"}""");
    val intSchema = new Schema.Parser().parse("""{"type": "int"}""");
    val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
    val schemaManager = RegistryClientFactory.create(registryConfig)
    val schemaId = schemaManager.register("deserialize-string-correctly", strSchema)
    val data = Seq(Row(asString(intSchema, 1)))

    val result = spark.createDataFrame(data.asJava, StructType(Seq(StructField("key", StringType))))
      .select(expr(
        s"""
           |from_avro_json(
           | key, 
           | ${schemaId}, 
           | map(
           |   'subject', 'deserialize-string-correctly', 
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

  it should "work as sql query with id" in {
    import spark.implicits._
    val strSchema = new Schema.Parser().parse("""{"type": "string"}""");
    val intSchema = new Schema.Parser().parse("""{"type": "int"}""");
    val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
    val schemaManager = RegistryClientFactory.create(registryConfig)
    val schemaId = schemaManager.register("deserialize-string-correctly", strSchema)
    val data = Seq(Row(asString(intSchema, 1)))
    val randomViewId = UUID.randomUUID.toString.replace("-", "")
    spark.createDataFrame(data.asJava, StructType(Seq(StructField("key", StringType)))).createTempView(randomViewId)
    val result = spark.sql(
      s"""
         |SELECT
         |  from_avro_json(
         |   key, 
         |   ${schemaId}, 
         |   map(
         |     'subject', 'deserialize-string-correctly', 
         |     'errOnEvolution', 'false', 
         |     'errHandler', 'com.adobe.spark.sql.avro.errors.DefaultRecordExceptionHandler', 
         |     'magicByteSize', '4'
         |   ), 
         |   'HELLO', 
         |   map('schema.registry.url', 'mock://registry', 'max.schemas.per.subject', '200')
         |  ) AS value
         |FROM `${randomViewId}`
         |""".stripMargin
    )

    val expected = Seq("HELLO").toDF("value")
    result.show(false)
    expected.show(false)
    assertSmallDatasetEquality(result, expected)
  }

  it should "invalid schema id with id col throws InvalidSchemaIdError" in {
    import spark.implicits._
    val schema = new Schema.Parser().parse("""{"type": "string"}""");
    val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
    val config = AvroDeSerConfig(Int.MaxValue, schema, errOnEvolution = false, FailFastExceptionHandler())
    val data = Seq(Row(asString(schema, "key")))
    val result = spark.createDataFrame(data.asJava, StructType(Seq(StructField("key", StringType))))
      .select(new Column(AvroJsonToCatalyst(functions.col("key").expr, functions.lit(Int.MaxValue).expr, config, registryConfig)).as("key"))
    val error = intercept[SparkException] {
      result.show(false)
    }
    assert(error.getCause != null)
    assert(error.getCause.getClass == classOf[InvalidSchemaIdError])
  }

  it should "invalid url with id col throws RegistryCallError" in {
    import spark.implicits._
    val schema = new Schema.Parser().parse("""{"type": "string"}""");
    val registryConfig = Map("schema.registry.url" -> "ftp://registry", "max.schemas.per.subject" -> "200")
    val config = AvroDeSerConfig(Int.MaxValue, schema, errOnEvolution = false, FailFastExceptionHandler())
    val data = Seq(Row(asString(schema, "key")))
    val result = spark.createDataFrame(data.asJava, StructType(Seq(StructField("key", StringType))))
      .select(new Column(AvroJsonToCatalyst(functions.col("key").expr, functions.lit(Int.MaxValue).expr, config, registryConfig)).as("key"))
    assertThrows[RegistryCallError] {
      result.show(false)
    }
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

