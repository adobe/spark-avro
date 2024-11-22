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

import com.adobe.spark.sql.avro.config.Config
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, MockSchemaRegistryClient, SchemaRegistryClient}

import java.util

object RegistryClient {
  
  def createClient(configs: Map[String, String]): SchemaRegistryClient = {
    val conf = new util.HashMap[String, Object]()
    configs.foreach { case (key, value) => conf.put(key, value) }
    val registryUrl = conf.remove(Config.SCHEMA_REGISTRY_URL).asInstanceOf[String]
    val cacheSize = conf.remove(Config.CACHE_SIZE).asInstanceOf[String].toInt
    if (registryUrl.startsWith("mock://")) {
      new MockSchemaRegistryClient().asInstanceOf[SchemaRegistryClient]
    } else {
      new CachedSchemaRegistryClient(registryUrl, cacheSize).asInstanceOf[SchemaRegistryClient]
    }
  }

}