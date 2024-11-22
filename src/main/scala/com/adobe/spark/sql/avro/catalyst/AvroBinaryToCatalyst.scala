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
import com.adobe.spark.sql.avro.config.{AvroDeSerConfig, Config}
import com.adobe.spark.sql.avro.errors.{DeSerExceptionHandler, SchemaEvolutionError}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.commons.lang3.SerializationException
import org.apache.spark.sql.avro.{DelegatingAvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, ExprUtils, Expression, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType}

import java.nio.ByteBuffer
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class AvroBinaryToCatalyst(data: Expression,
                                config: AvroDeSerConfig,
                                registryConfig: Map[String, String]) 
  extends UnaryExpression with ExpectsInputTypes {
  
  override def child: Expression = data
  
  override def inputTypes: Seq[DataType] = BinaryType :: Nil

  override lazy val dataType: DataType = SchemaConverters.toSqlType(readerSchema).dataType

  override def nullable: Boolean = true
  
  @transient private lazy val registry = RegistryFactory.create(registryConfig)

  @transient private lazy val readerSchema = config.schema
  
  @transient private lazy val deserializationHandler: DeSerExceptionHandler = config.errHandler
  
  @transient private lazy val readerCache: mutable.HashMap[Int, GenericDatumReader[Any]] =
    new mutable.HashMap[Int, GenericDatumReader[Any]]()

  @transient private var decoder: BinaryDecoder = _

  @transient private lazy val deserializer = new DelegatingAvroDeserializer(readerSchema, dataType)

  @transient private val MAGIC_BYTE = 0x0

  @transient private val MAGIC_BYTE_SIZE = config.magicByteSize

  // Reused the result
  @transient private var result: Any = _
  
  def this(data: Expression,
           config: Expression,
           default: Expression, 
           registryConfig: Expression) = {
    this(
      data, 
      Config.avroDeSerConfigFromMap(ExprUtils.convertToMapData(config), Option.apply(default.eval())), 
      ExprUtils.convertToMapData(registryConfig)
    )
  }
  
  private def decodeAvro(payload: Array[Byte]): Any = {
    val buffer = ByteBuffer.wrap(payload)
    if (buffer.get() != MAGIC_BYTE) {
      throw new SerializationException("Invalid magic byte!")
    }
    val schemaId = MAGIC_BYTE_SIZE match {
      case 4 => buffer.getInt
      case 8 => buffer.getLong.toInt
    }
    val start = buffer.position() + buffer.arrayOffset()
    val length = buffer.limit() - 1 - MAGIC_BYTE_SIZE
    decoder = DecoderFactory.get().binaryDecoder(buffer.array(), start, length, decoder)
    val reader = readerCache.getOrElseUpdate(schemaId, {
      new GenericDatumReader[Any](fetchWriterSchema(schemaId), readerSchema)
    })
    result = reader.read(result, decoder)
    checkAndThrowIfSchemaHasEvolved(schemaId)
    result
  }

  private def fetchWriterSchema(id: Int): Schema = {
    Try(registry.getSchemaById(id)) match {
      case Success(parsedSchema) => {
        parsedSchema match {
          case schema: AvroSchema => schema.rawSchema()
          case _ => throw new UnsupportedOperationException(s"AvroBinaryToCatalyst only supports AvroSchema")
        }
      }
      case Failure(e) => throw new RuntimeException("Not able to download writer schema", e)
    }
  }

  private def checkAndThrowIfSchemaHasEvolved(writerSchemaId: Int): Unit = {
    if (config.errOnEvolution && writerSchemaId > config.schemaId
    ) {
      throw SchemaEvolutionError(
        s"Schema has evolved, need to restart the application. ${writerSchemaId} vs ${config.schemaId}");
    }
  }

  override def nullSafeEval(input: Any): Any = {
    try {
      deserializer.deserialize(decodeAvro(input.asInstanceOf[Array[Byte]]))
    } catch {
      case e: SchemaEvolutionError => throw e
      case e: Exception => deserializationHandler.handle(e, deserializer, readerSchema)
      case e: Throwable => deserializationHandler.handle(e, deserializer, readerSchema)
    }
  }

  override protected def flatArguments: Iterator[Any] = super.flatArguments.filter(_ => true)
  
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(ctx, ev, (eval) => {
      val result = ctx.freshName("result")
      val dt = CodeGenerator.boxedType(dataType)
      s"""
      $dt $result = ($dt) $expr.nullSafeEval($eval);
      if ($result == null) {
        ${ev.isNull} = true;
      } else {
        ${ev.value} = $result;
      }
    """
    })
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(newChild, config)
  }

  override def prettyName: String = "from_avro_using_registry" // Very innovative naming

}