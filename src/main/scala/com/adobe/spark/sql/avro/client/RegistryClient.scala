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
import com.adobe.spark.sql.avro.errors.{InvalidSchemaIdError, RegistryCallError}
import io.apicurio.registry.rest.client.{RegistryClientFactory => ApicurioClientFactory, RegistryClient => ApicurioClient}
import io.apicurio.registry.types.ArtifactType
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

case class SchemaMetadata(id: Long, version: Int)

trait RegistryClient {
  def getSchemaById(id: Long): Schema
  def getLatestMetadata(subject: String): SchemaMetadata
  def register(subject: String, schema: Schema): Long
  def getSchemaBySubject(subject: String): Schema = getSchemaById(getSchemaId(subject).toInt)

  def getSchemaId(subject: String): Long = getLatestMetadata(subject).id

}

object RegistryClient extends Logging {
  def createClient(configs: Map[String, String]): RegistryClient = {
    val conf = new util.HashMap[String, Object]()
    configs.foreach { case (key, value) => conf.put(key, value) }
    val registryUrl = Option(conf.remove(Config.SCHEMA_REGISTRY_URL)) match {
      case Some(url: String) => url
      case Some(url: Any) => throw new IllegalArgumentException("Registry url should be of type string")
      case None => throw new IllegalArgumentException("Registry url missing from config")
    }
    if (registryUrl.startsWith("mock://")) {
      new MockSchemaRegistryClient().asInstanceOf[RegistryClient]
    } else {
      try {
        val clazz = Class.forName(Option(conf.remove("class")).map(_.asInstanceOf[String]).getOrElse("com.adobe.spark.sql.avro.client.ApicurioRegistryClient"))
        log.info(s"Creating registry with class '${clazz.getCanonicalName}'")
        Try(clazz.getConstructor(classOf[String], classOf[Map[String, String]]).newInstance(registryUrl, configs))
          .recover({case _: NoSuchMethodException => clazz.getConstructor().newInstance()})
          .get
          .asInstanceOf[RegistryClient]
      } catch {
        case e if NonFatal(e) =>
          throw new IllegalArgumentException(s"Registry class must have a constructor accepting (String, Map[String, String]) parameters or a constructor with no parameters", e)
      }
    }
  }
}

case class ConfluentRegistryClient(url: String, configs: Map[String, Object]) extends RegistryClient with Logging {
  
  lazy val client: SchemaRegistryClient = {
    val conf = mutable.HashMap[String, Object]()
    configs.foreach { case (key, value) => conf.put(key, value) }
    val cacheSize = Option(conf.remove(Config.CACHE_SIZE)).getOrElse(32).asInstanceOf[String].toInt
    new CachedSchemaRegistryClient(url, cacheSize, new util.HashMap[String, Object](configs.asJava)).asInstanceOf[SchemaRegistryClient]

  }

  override def getSchemaById(id: Long): Schema = client.getSchemaById(id.toInt).rawSchema.asInstanceOf[Schema]
  
  override def getLatestMetadata(subject: String): SchemaMetadata = {
    val meta = client.getLatestSchemaMetadata(subject)
    SchemaMetadata(meta.getId, meta.getVersion)
  }

  override def register(subject: String, schema: Schema): Long = client.register(subject, new AvroSchema(schema))
}

case class ApicurioRegistryClient(url: String, configs: Map[String, Object]) extends RegistryClient with Logging {

  private val APICURIO_DEFAULT_GROUP_ID = "default"

  lazy val client: ApicurioClient = ApicurioClientFactory.create(url, new util.HashMap[String, Object](configs.asJava))

  override def getSchemaById(schemaId: Long): Schema = {
    val schemaStream = client.getContentByGlobalId(schemaId, true, true)

    var schemaString = ""
    try {
      schemaString = IOUtils.toString(schemaStream, StandardCharsets.UTF_8)
      new Schema.Parser().parse(schemaString)
    } catch {
      case e: Exception => {
        log.error(s"Error occurred when getting/parsing the schema ${schemaId} ${schemaString}", e)
        throw e
      }
    }
  }

  override def getLatestMetadata(subject: String): SchemaMetadata = {
    val metadata = client.getArtifactMetaData(APICURIO_DEFAULT_GROUP_ID, subject)
    SchemaMetadata(metadata.getGlobalId, metadata.getVersion.toInt)
  }

  override def register(subject: String, schema: Schema): Long = {
    val stream = new java.io.ByteArrayInputStream(schema.toString.getBytes(java.nio.charset.StandardCharsets.UTF_8.name))
    val artifactMetaData = client.createArtifact("default", subject, ArtifactType.AVRO, stream)
    artifactMetaData.getGlobalId
  }

}

class MockSchemaRegistryClient extends RegistryClient with Logging {
  
  lazy val client = new io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient()
  
  @throws[IOException]
  @throws[RestClientException]
  override def getSchemaById(id: Long): Schema = {
    try {
      client.getSchemaById(id.toInt).rawSchema.asInstanceOf[Schema]
    } catch {
      case e: IOException if e.getMessage.contains("Cannot get schema from schema registry!") => throw new RestClientException("No schema registered under subject!", 404, 40401)
      case e: IOException if e.getMessage.contains("No artifact with ID") => throw new RestClientException("No schema registered under subject!", 404, 40401)
      case e: IOException if e.getMessage == "No schema registered under subject!" => throw new RestClientException("No schema registered under subject!", 404, 40401)
    }
  }
  
  override def getLatestMetadata(subject: String): SchemaMetadata = {
    val meta = client.getLatestSchemaMetadata(subject)
    SchemaMetadata(meta.getId, meta.getVersion)
  }

  override def register(subject: String, schema: Schema): Long = client.register(subject, new AvroSchema(schema))
}

private[client] case class RegistryClientWrapper(registryClient: RegistryClient) extends RegistryClient {
  override def getSchemaById(id: Long): Schema = {
    Try(registryClient.getSchemaById(id.toInt)) match {
      case Success(schema: Schema) => schema
      case Failure(e: RestClientException) => e.getErrorCode match {
        case 40401 => throw InvalidSchemaIdError(id, e)
        case 40403 => throw InvalidSchemaIdError(id, e)
        case _: Int => throw RegistryCallError("Call to registry failed with", e)
      }
      case Failure(e) => throw RegistryCallError("Not able to download writer schema", e)
    }
  }

  override def register(subject: String, schema: Schema): Long = {
    registryClient.register(subject, schema)
  }

  override def getLatestMetadata(subject: String): SchemaMetadata = {
    registryClient.getLatestMetadata(subject)
  }
}
