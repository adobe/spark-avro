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

import com.adobe.spark.sql.avro.config.{AvroSerConfig, Config}
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.{BinaryEncoder, EncoderFactory, JsonEncoder}
import org.apache.spark.sql.avro.{DelegatingAvroSerializer, SchemaConverters}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, ExprUtils, Expression, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

case class CatalystToAvroBinary(data: Expression,
                                config: AvroSerConfig,
                                registryConfig: Map[String, String])
  extends UnaryExpression with ExpectsInputTypes {

  override def child: Expression = data

  override def dataType: DataType = BinaryType

  override def inputTypes: Seq[DataType] = Seq(SchemaConverters.toSqlType(config.schema).dataType)

  @transient private lazy val serializer = new DelegatingAvroSerializer(config.schema, child.dataType, data.nullable)

  @transient private lazy val writer = new GenericDatumWriter[Any](writerSchema)

  @transient private lazy val writerSchema = config.schema

  @transient private var encoder: BinaryEncoder = _

  @transient private lazy val out = new ByteArrayOutputStream

  private val MAGIC_BYTE = 0x0
  
  def this(data: Expression,
           config: Expression,
           registryConfig: Expression) = {
    this(
      data,
      Config.avroSerConfigFromMap(ExprUtils.convertToMapData(config), ExprUtils.convertToMapData(registryConfig)),
      ExprUtils.convertToMapData(registryConfig)
    )
  }

  override def nullSafeEval(input: Any): Any = {
    out.reset()
    if (config.writeSchemaId) {
      writeSchemaId(config.schemaId, out)
    }
    encoder = EncoderFactory.get().directBinaryEncoder(out, encoder)
    val avroData = serializer.serialize(input)
    writer.write(avroData, encoder)
    encoder.flush()
    out.toByteArray
  }

  private def writeSchemaId(id: Long, outStream: ByteArrayOutputStream): Unit = {
    outStream.write(MAGIC_BYTE)
    if (config.magicByteSize == 4)
      outStream.write(ByteBuffer.allocate(config.magicByteSize).putInt(id.toInt).array())
    else
      outStream.write(ByteBuffer.allocate(config.magicByteSize).putLong(id).array())
  }

  override protected def flatArguments: Iterator[Any] = super.flatArguments

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(byte[]) $expr.nullSafeEval($input)")
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(data = newChild)

  override def prettyName: String = "to_avro_binary" // Very innovative naming

}

case class CatalystToAvroJson(data: Expression,
                                config: AvroSerConfig,
                                registryConfig: Map[String, String])
  extends UnaryExpression with ExpectsInputTypes {

  override def child: Expression = data

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(SchemaConverters.toSqlType(config.schema).dataType)

  @transient private lazy val serializer = new DelegatingAvroSerializer(config.schema, child.dataType, data.nullable)

  @transient private lazy val writer = new GenericDatumWriter[Any](writerSchema)

  @transient private lazy val writerSchema = config.schema

  @transient private var encoder: JsonEncoder = _

  @transient private lazy val out = new ByteArrayOutputStream
  
  def this(data: Expression,
           config: Expression,
           registryConfig: Expression) = {
    this(
      data,
      Config.avroSerConfigFromMap(ExprUtils.convertToMapData(config), ExprUtils.convertToMapData(registryConfig)),
      ExprUtils.convertToMapData(registryConfig)
    )
  }

  override def nullSafeEval(input: Any): UTF8String = {
    out.reset()
    encoder = EncoderFactory.get().jsonEncoder(writerSchema, out)
    val avroData = serializer.serialize(input)
    writer.write(avroData, encoder)
    encoder.flush()
    UTF8String.fromString(out.toString)
  }

  override protected def flatArguments: Iterator[Any] = super.flatArguments

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(UTF8String) $expr.nullSafeEval($input)")
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(data = newChild)

  override def prettyName: String = "to_avro_json"

}
