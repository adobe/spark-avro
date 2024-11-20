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

package com.adobe.spark.sql.avro.client

import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

case class SchemaMetadata(id: Long, version: Int)

trait Registry {
  def getSchemaById(id: Long): ParsedSchema
  def getLatestMetadata(subject: String): SchemaMetadata
  def register(subject: String, parsedSchema: ParsedSchema): Long
}

private[client] case class RegistryClientWrapper(registryClient: SchemaRegistryClient) extends Registry {
  override def getSchemaById(id: Long): ParsedSchema = {
    registryClient.getSchemaById(id.toInt)
  }

  override def register(subject: String, parsedSchema: ParsedSchema): Long = {
    registryClient.register(subject, parsedSchema).toLong
  }

  override def getLatestMetadata(subject: String): SchemaMetadata = {
    val meta = registryClient.getLatestSchemaMetadata(subject)
    SchemaMetadata(meta.getId, meta.getVersion)
  }
}
