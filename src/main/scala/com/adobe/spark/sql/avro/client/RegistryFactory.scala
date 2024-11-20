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

import scala.collection.concurrent

object RegistryFactory {
  private val clientInstances: concurrent.Map[Map[String,String], Registry] = concurrent.TrieMap()
  
  def create(configs: Map[String,String]): Registry = {
    clientInstances.getOrElseUpdate(configs, {
      RegistryClientWrapper(RegistryClient.createClient(configs))
    })
  }

}
