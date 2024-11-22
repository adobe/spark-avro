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

package com.adobe.spark.sql.avro.config

import com.adobe.spark.sql.avro.errors.{DeSerExceptionHandler, FailFastExceptionHandler}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.Schema


case class AvroDeSerConfig(schemaId: Long,
                           schema: Schema,
                           errOnEvolution: Boolean = false,
                           errHandler: DeSerExceptionHandler = FailFastExceptionHandler(),
                           magicByteSize: Int = 4) extends Serializable {
}

case class AvroSerConfig(schemaId: Long,
                         schema: Schema,
                         writeSchemaId: Boolean = true,
                         magicByteSize: Int = 4) extends Serializable {
}

object Config {
  val SCHEMA_REGISTRY_URL = "schema.registry.url"
  val CACHE_SIZE = "max.schemas.per.subject"

  def avroDeSerConfigFromMap(map: Map[String, String], default: Option[Any]): AvroDeSerConfig = {
    AvroDeSerConfig(
      schemaId = map("schemaId").toLong,
      schema = new AvroSchema(map("schema")).rawSchema(),
      errOnEvolution = map("errOnEvolution").toBoolean,
      errHandler = map("errHandler") match {
        case klass: String => DeSerExceptionHandler.build(klass, default)
        case _ => throw new IllegalArgumentException("Missing errHandler from config")
      },
      magicByteSize = map.getOrElse("magicByteSize", "4").toInt
    )
  }

  def avroSerConfigFromMap(map: Map[String, String]): AvroSerConfig = {
    AvroSerConfig(
      schemaId = map("schemaId").toLong,
      schema = new AvroSchema(map("schema")).rawSchema(),
      writeSchemaId = map("writeSchemaId").toBoolean,
      magicByteSize = map.getOrElse("magicByteSize", "4").toInt
    )
  }

}
