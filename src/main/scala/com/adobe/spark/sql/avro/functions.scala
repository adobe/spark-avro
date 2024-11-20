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

import com.adobe.spark.sql.avro.catalyst.{AvroBinaryToCatalyst, CatalystToAvroBinary}
import com.adobe.spark.sql.avro.client.RegistryFactory
import com.adobe.spark.sql.avro.config.{AvroDeSerConfig, AvroSerConfig}
import com.adobe.spark.sql.avro.errors.{DeSerExceptionHandler, FailFastExceptionHandler}
import org.apache.avro.Schema
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

object functions {

  def from_avro(data: Expression, config: AvroDeSerConfig, registryConfig: Map[String, String]): Column = {
    new Column(AvroBinaryToCatalyst(data, config, registryConfig))
  }

  def to_avro(data: Expression, config: AvroSerConfig, registryConfig: Map[String, String]): Column = {
    new Column(CatalystToAvroBinary(data, config, registryConfig))
  }

  def serConfig(schemaId: Long,
                registryConfig: Map[String, String],
                writeSchemaId: Boolean = true,
                magicByteSize: Int = 4): AvroSerConfig = {
    val client = RegistryFactory.create(registryConfig)
    AvroSerConfig(
      schemaId = schemaId,
      schema = client.getSchemaById(schemaId).rawSchema.asInstanceOf[Schema],
      writeSchemaId = writeSchemaId,
      magicByteSize = magicByteSize)
  }

  def deSerConfig(schemaId: Long,
                  registryConfig: Map[String, String],
                  errOnEvolution: Boolean = false,
                  errHandler: DeSerExceptionHandler = FailFastExceptionHandler(),
                  magicByteSize: Int = 4): AvroDeSerConfig = {
    val client = RegistryFactory.create(registryConfig)
    AvroDeSerConfig(
      schemaId = schemaId,
      schema = client.getSchemaById(schemaId).rawSchema.asInstanceOf[Schema],
      errOnEvolution = errOnEvolution,
      errHandler = errHandler,
      magicByteSize = magicByteSize
    )
  }
  
  def serConfigForSubject(subject: String,
                registryConfig: Map[String, String],
                writeSchemaId: Boolean = true,
                magicByteSize: Int = 4): AvroSerConfig = {
    val client = RegistryFactory.create(registryConfig)
    val meta = client.getLatestMetadata(subject)
    AvroSerConfig(
      schemaId = meta.id,
      schema = client.getSchemaById(meta.id).rawSchema.asInstanceOf[Schema],
      writeSchemaId = writeSchemaId,
      magicByteSize = magicByteSize)
  }

  def deSerConfigForSubject(subject: String,
                  registryConfig: Map[String, String],
                  errOnEvolution: Boolean = false,
                  errHandler: DeSerExceptionHandler = FailFastExceptionHandler(),
                  magicByteSize: Int = 4): AvroDeSerConfig = {
    val client = RegistryFactory.create(registryConfig)
    val meta = client.getLatestMetadata(subject)
    AvroDeSerConfig(
      schemaId = meta.id,
      schema = client.getSchemaById(meta.id).rawSchema.asInstanceOf[Schema],
      errOnEvolution = errOnEvolution,
      errHandler = errHandler,
      magicByteSize = magicByteSize
    )
  }
  
}
