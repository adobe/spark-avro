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

package com.adobe.spark.sql.avro

import com.adobe.spark.sql.avro.catalyst.{AvroBinaryToCatalyst, AvroBinaryWithIdToCatalyst, AvroJsonToCatalyst, CatalystToAvroBinary, CatalystToAvroJson}
import com.adobe.spark.sql.avro.client.RegistryClientFactory
import com.adobe.spark.sql.avro.config.{AvroDeSerConfig, AvroSerConfig}
import com.adobe.spark.sql.avro.errors.{DeSerExceptionHandler, FailFastExceptionHandler}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{Column, SparkSession}

import scala.collection.convert.ImplicitConversions.`map AsScala`

object functions {

  def registerFunctions() = {
    SparkSession.active.sessionState.functionRegistry
      .registerFunction(FunctionIdentifier("from_avro_binary"),
        (children: Seq[Expression]) => new AvroBinaryToCatalyst(children.head, children(1), children(2), children.last),
        "built-in"
      )
    SparkSession.active.sessionState.functionRegistry
      .registerFunction(FunctionIdentifier("from_avro_binary_with_id"),
        (children: Seq[Expression]) => new AvroBinaryWithIdToCatalyst(children.head, children(1), children(2), children(3), children.last),
        "built-in"
      )
    SparkSession.active.sessionState.functionRegistry
      .registerFunction(FunctionIdentifier("from_avro_json"),
        (children: Seq[Expression]) => new AvroJsonToCatalyst(children.head, children(1), children(2), children(3), children.last),
        "built-in"
      )
    SparkSession.active.sessionState.functionRegistry
      .registerFunction(FunctionIdentifier("to_avro_binary"),
        (children: Seq[Expression]) => new CatalystToAvroBinary(children.head, children(1), children.last),
        "built-in"
      )
    SparkSession.active.sessionState.functionRegistry
      .registerFunction(FunctionIdentifier("to_avro_json"),
        (children: Seq[Expression]) => new CatalystToAvroJson(children.head, children(1), children.last),
        "built-in"
      )
  }
  
  def from_avro(data: Column, config: AvroDeSerConfig, registryConfig: Map[String, String]): Column = {
    new Column(AvroBinaryToCatalyst(data.expr, config, registryConfig))
  }

  def from_avro(data: Column, config: AvroDeSerConfig, registryConfig: java.util.Map[String, String]): Column = {
    new Column(AvroBinaryToCatalyst(data.expr, config, registryConfig.toMap))
  }

  def from_avro(data: Column, schemaId: Column, config: AvroDeSerConfig, registryConfig: Map[String, String]): Column = {
    new Column(AvroBinaryWithIdToCatalyst(data.expr, schemaId.expr, config, registryConfig))
  }

  def from_avro(data: Column, schemaId: Column, config: AvroDeSerConfig, registryConfig: java.util.Map[String, String]): Column = {
    new Column(AvroBinaryWithIdToCatalyst(data.expr, schemaId.expr, config, registryConfig.toMap))
  }

  def from_avro_json(data: Column, schemaId: Column, config: AvroDeSerConfig, registryConfig: Map[String, String]): Column = {
    new Column(AvroJsonToCatalyst(data.expr, schemaId.expr, config, registryConfig))
  }

  def from_avro_json(data: Column, schemaId: Column, config: AvroDeSerConfig, registryConfig: java.util.Map[String, String]): Column = {
    new Column(AvroJsonToCatalyst(data.expr, schemaId.expr, config, registryConfig.toMap))
  }

  def to_avro(data: Column, config: AvroSerConfig, registryConfig: Map[String, String]): Column = {
    new Column(CatalystToAvroBinary(data.expr, config, registryConfig))
  }

  def to_avro(data: Column, config: AvroSerConfig, registryConfig: java.util.Map[String, String]): Column = {
    new Column(CatalystToAvroBinary(data.expr, config, registryConfig.toMap))
  }

  def to_avro_json(data: Column, config: AvroSerConfig, registryConfig: Map[String, String]): Column = {
    new Column(CatalystToAvroJson(data.expr, config, registryConfig))
  }

  def to_avro_json(data: Column, config: AvroSerConfig, registryConfig: java.util.Map[String, String]): Column = {
    new Column(CatalystToAvroJson(data.expr, config, registryConfig.toMap))
  }

  def serConfig(schemaId: Long,
                registryConfig: Map[String, String],
                writeSchemaId: Boolean = true,
                magicByteSize: Int = 4): AvroSerConfig = {
    val client = RegistryClientFactory.create(registryConfig)
    AvroSerConfig(
      schemaId = schemaId,
      schema = client.getSchemaById(schemaId),
      writeSchemaId = writeSchemaId,
      magicByteSize = magicByteSize)
  }

  def deSerConfig(schemaId: Long,
                  registryConfig: Map[String, String],
                  errOnEvolution: Boolean = false,
                  errHandler: DeSerExceptionHandler = FailFastExceptionHandler(),
                  magicByteSize: Int = 4): AvroDeSerConfig = {
    val client = RegistryClientFactory.create(registryConfig)
    AvroDeSerConfig(
      schemaId = schemaId,
      schema = client.getSchemaById(schemaId),
      errOnEvolution = errOnEvolution,
      errHandler = errHandler,
      magicByteSize = magicByteSize
    )
  }
  
  def serConfigForSubject(subject: String,
                registryConfig: Map[String, String],
                writeSchemaId: Boolean = true,
                magicByteSize: Int = 4): AvroSerConfig = {
    val client = RegistryClientFactory.create(registryConfig)
    val meta = client.getLatestMetadata(subject)
    AvroSerConfig(
      schemaId = meta.id,
      schema = client.getSchemaById(meta.id),
      writeSchemaId = writeSchemaId,
      magicByteSize = magicByteSize)
  }

  def deSerConfigForSubject(subject: String,
                  registryConfig: Map[String, String],
                  errOnEvolution: Boolean = false,
                  errHandler: DeSerExceptionHandler = FailFastExceptionHandler(),
                  magicByteSize: Int = 4): AvroDeSerConfig = {
    val client = RegistryClientFactory.create(registryConfig)
    val meta = client.getLatestMetadata(subject)
    AvroDeSerConfig(
      schemaId = meta.id,
      schema = client.getSchemaById(meta.id),
      errOnEvolution = errOnEvolution,
      errHandler = errHandler,
      magicByteSize = magicByteSize
    )
  }
  
}
