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
import com.adobe.spark.sql.avro.config.{AvroDeSerConfig, Config}
import com.adobe.spark.sql.avro.errors.{DeSerExceptionHandler, InvalidSchemaIdError, RegistryCallError, SchemaEvolutionError}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.{BinaryDecoder, Decoder, DecoderFactory}
import org.apache.commons.lang3.SerializationException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.{DelegatingAvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, ExprUtils, Expression, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, StringType}

import java.nio.ByteBuffer
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

trait AbstractAvroToCatalyst extends Logging {
  
  val data: Expression
  val config: AvroDeSerConfig
  val registryConfig: Map[String, String]
  val dataType: DataType

  @transient protected var decoder: Decoder = _
  @transient protected var result: Any = _ // Reused the result
  @transient protected lazy val readerSchema: Schema = config.schema
  @transient protected lazy val readerCache: mutable.HashMap[Int, GenericDatumReader[Any]] = new mutable.HashMap[Int, GenericDatumReader[Any]]()
  @transient protected lazy val deserializationHandler: DeSerExceptionHandler = config.errHandler
  @transient protected lazy val deserializer = new DelegatingAvroDeserializer(readerSchema, dataType)

  @transient private lazy val badSchemaIds = new mutable.HashSet[Int]()
  @transient private lazy val registry = RegistryClientFactory.create(registryConfig)

  protected def decodeAvroBinary(buffer: ByteBuffer, schemaId: Int, start: Int, length: Int): Any = {
    decoder = DecoderFactory.get().binaryDecoder(buffer.array(), start, length, decoder.asInstanceOf[BinaryDecoder])
    val reader = readerCache.getOrElseUpdate(schemaId, {
      new GenericDatumReader[Any](fetchWriterSchema(schemaId), readerSchema)
    })
    result = reader.read(result, decoder)
    checkAndThrowIfSchemaHasEvolved(schemaId)
    result
  }

  protected def decodeAvroJson(payload: String, schemaId: Int): Any = {
    decoder = DecoderFactory.get().jsonDecoder(readerSchema, payload)
    val reader = readerCache.getOrElseUpdate(schemaId, {
      new GenericDatumReader[Any](fetchWriterSchema(schemaId), readerSchema)
    })
    result = reader.read(result, decoder)
    checkAndThrowIfSchemaHasEvolved(schemaId)
    result
  }

  protected def fetchWriterSchema(id: Int): Schema = {
    if (badSchemaIds.contains(id)) {
      throw InvalidSchemaIdError(id)
    }
    Try(registry.getSchemaById(id)) match {
      case Success(parsedSchema) => {
        parsedSchema match {
          case schema: Schema => schema
          case _ => throw new UnsupportedOperationException(s"AvroBinaryToCatalyst only supports AvroSchema")
        }
      }
      case Failure(e: InvalidSchemaIdError) => {
        badSchemaIds.add(id)
        throw e
      }
      case Failure(e: RegistryCallError) => throw e
      case Failure(e: Throwable) => throw new RuntimeException("Not able to download writer schema", e)
    }
  }

  protected def checkAndThrowIfSchemaHasEvolved(writerSchemaId: Int): Unit = {
    if (config.errOnEvolution && writerSchemaId > config.schemaId
    ) {
      throw SchemaEvolutionError(
        s"Schema has evolved, need to restart the application. ${writerSchemaId} vs ${config.schemaId}");
    }
  }

  protected def withTrace[T](e: Throwable)(block: => T): T = {
    log.trace("Something went wrong!", e) // your logging method
    block // execute the block passed in
  }

}

case class AvroBinaryToCatalyst(data: Expression,
                                config: AvroDeSerConfig,
                                registryConfig: Map[String, String]) 
  extends UnaryExpression with ExpectsInputTypes with AbstractAvroToCatalyst {
  
  override def child: Expression = data
  
  override def inputTypes: Seq[DataType] = BinaryType :: Nil

  override lazy val dataType: DataType = SchemaConverters.toSqlType(readerSchema).dataType

  override def nullable: Boolean = true

  private val MAGIC_BYTE = 0x0
  
  // Reused the result
  def this(data: Expression,
           config: Expression,
           default: Expression, 
           registryConfig: Expression) = {
    this(
      data, 
      Config.avroDeSerConfigFromMap(ExprUtils.convertToMapData(config), Option.apply(default.eval()), ExprUtils.convertToMapData(registryConfig)), 
      ExprUtils.convertToMapData(registryConfig)
    )
  }
  
  private def decodeAvro(payload: Array[Byte]): Any = {
    val buffer = ByteBuffer.wrap(payload)
    if (buffer.get() != MAGIC_BYTE) {
      throw new SerializationException("Invalid magic byte!")
    }
    val schemaId = config.magicByteSize match {
      case 4 => buffer.getInt
      case 8 => buffer.getLong.toInt
    }
    decodeAvroBinary(buffer, schemaId, buffer.position() + buffer.arrayOffset(), buffer.limit() - 1 - config.magicByteSize)
  }
  
  override def nullSafeEval(input: Any): Any = {
    try {
      deserializer.deserialize(decodeAvro(input.asInstanceOf[Array[Byte]]))
    } catch {
      case e: SchemaEvolutionError => withTrace(e) {throw e}
      case e: RegistryCallError => withTrace(e) {throw e}
      case e: Exception => withTrace(e) {deserializationHandler.handle(e, deserializer, readerSchema)}
      case e: Throwable => withTrace(e) {deserializationHandler.handle(e, deserializer, readerSchema)}
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

  override def prettyName: String = "from_avro_binary" // Very innovative naming

}


case class AvroBinaryWithIdToCatalyst(data: Expression,
                                      schemaId: Expression,
                                      config: AvroDeSerConfig,
                                      registryConfig: Map[String, String])
  extends BinaryExpression with ExpectsInputTypes with Logging with AbstractAvroToCatalyst {

  override def left: Expression = data

  override def right: Expression = schemaId

  override def inputTypes: Seq[DataType] = Seq(BinaryType, IntegerType)

  override lazy val dataType: DataType = SchemaConverters.toSqlType(readerSchema).dataType

  override def nullable: Boolean = true

  def this(data: Expression,
           schemaId: Expression,
           config: Expression,
           default: Expression,
           registryConfig: Expression) = {
    this(
      data,
      schemaId,
      Config.avroDeSerConfigFromMap(ExprUtils.convertToMapData(config), Option.apply(default.eval()), ExprUtils.convertToMapData(registryConfig)),
      ExprUtils.convertToMapData(registryConfig)
    )
  }

  private def decodeAvro(payload: Array[Byte], schemaId: Int): Any = {
    val buffer = ByteBuffer.wrap(payload)
    decodeAvroBinary(buffer, schemaId, 0, buffer.limit())
  }

  override def nullSafeEval(input: Any, writerSchemaId: Any): Any = {
    try {
      deserializer.deserialize(decodeAvro(input.asInstanceOf[Array[Byte]], writerSchemaId.asInstanceOf[Int]))
    } catch {
      case e: SchemaEvolutionError => withTrace(e) {throw e}
      case e: RegistryCallError => withTrace(e) {throw e}
      case e: Exception => withTrace(e) {deserializationHandler.handle(e, deserializer, readerSchema)}
      case e: Throwable => withTrace(e) {deserializationHandler.handle(e, deserializer, readerSchema)}
    }
  }

  override protected def flatArguments: Iterator[Any] = super.flatArguments.filter(_ => true)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(ctx, ev, (data, schemaId) => {
      val result = ctx.freshName("result")
      val dt = CodeGenerator.boxedType(dataType)
      s"""
      $dt $result = ($dt) $expr.nullSafeEval($data, $schemaId);
      if ($result == null) {
        ${ev.isNull} = true;
      } else {
        ${ev.value} = $result;
      }
    """
    })
  }

  override protected def withNewChildrenInternal(newData: Expression, newSchemaId: Expression): Expression = {
    copy(newData, newSchemaId, config)
  }

  override def prettyName: String = "from_avro_binary_with_id"

}


case class AvroJsonToCatalyst(data: Expression, 
                              schemaId: Expression, 
                              config: AvroDeSerConfig, 
                              registryConfig: Map[String, String])
  extends BinaryExpression with ExpectsInputTypes with Logging with AbstractAvroToCatalyst {

  override def left: Expression = data

  override def right: Expression = schemaId

  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType)

  override lazy val dataType: DataType = SchemaConverters.toSqlType(readerSchema).dataType

  override def nullable: Boolean = true

  def this(data: Expression,
           schemaId: Expression,
           config: Expression,
           default: Expression,
           registryConfig: Expression) = {
    this(
      data,
      schemaId,
      Config.avroDeSerConfigFromMap(ExprUtils.convertToMapData(config), Option.apply(default.eval()), ExprUtils.convertToMapData(registryConfig)),
      ExprUtils.convertToMapData(registryConfig)
    )
  }
  
  override def nullSafeEval(input: Any, writerSchemaId: Any): Any = {
    try {
      deserializer.deserialize(decodeAvroJson(input.toString, writerSchemaId.asInstanceOf[Int]))
    } catch {
      case e: SchemaEvolutionError => withTrace(e) {throw e}
      case e: RegistryCallError => withTrace(e) {throw e}
      case e: Exception => withTrace(e) {deserializationHandler.handle(e, deserializer, readerSchema)}
      case e: Throwable => withTrace(e) {deserializationHandler.handle(e, deserializer, readerSchema)}
    }
  }

  override protected def flatArguments: Iterator[Any] = super.flatArguments.filter(_ => true)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(ctx, ev, (data, schemaId) => {
      val result = ctx.freshName("result")
      val dt = CodeGenerator.boxedType(dataType)
      s"""
      $dt $result = ($dt) $expr.nullSafeEval($data, $schemaId);
      if ($result == null) {
        ${ev.isNull} = true;
      } else {
        ${ev.value} = $result;
      }
    """
    })
  }

  override protected def withNewChildrenInternal(newData: Expression, newSchemaId: Expression): Expression = {
    copy(newData, newSchemaId, config)
  }

  override def prettyName: String = "from_avro_json"

}