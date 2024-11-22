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

package com.adobe.spark.sql.avro.errors

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.DelegatingAvroDeserializer

object DeSerExceptionHandler {
  def build(klass: String, default: Option[Any]): DeSerExceptionHandler = {
    default match {
      case Some(payload) => Class.forName(klass)
        .asSubclass(classOf[DeSerExceptionHandler])
        .getConstructor(classOf[Any]).newInstance(payload.asInstanceOf[Object])
      case _ => Class.forName(klass).newInstance().asInstanceOf[DeSerExceptionHandler]
    }
  }
}

trait DeSerExceptionHandler extends Serializable {
  
  def handle(exception: Throwable, deserializer: DelegatingAvroDeserializer, readerSchema: Schema): Any

  override def toString: String = {
    s"new ${getClass.getName}()";
  }

}


case class FailFastExceptionHandler() extends DeSerExceptionHandler {

  def handle(exception: Throwable, avroDeserializer: DelegatingAvroDeserializer, readerSchema: Schema): Any = {
    throw new SparkException("Malformed record detected. Fail fast.", exception)
  }
  
}


case class PermissiveRecordExceptionHandler() extends DeSerExceptionHandler with Logging {

  def handle(exception: Throwable, deserializer: DelegatingAvroDeserializer, readerSchema: Schema): Any = {
    logWarning("Malformed record detected. Return record with null data")
    deserializer.deserialize(new GenericData.Record(readerSchema))
  }

}

case class NullReturningRecordExceptionHandler() extends DeSerExceptionHandler with Logging {

  def handle(exception: Throwable, deserializer: DelegatingAvroDeserializer, readerSchema: Schema): Any = {
    logWarning("Malformed record detected. Return null")
    null
  }

}

case class DefaultRecordExceptionHandler(default: Any) extends DeSerExceptionHandler with Logging {

  def handle(exception: Throwable, deserializer: DelegatingAvroDeserializer, readerSchema: Schema): Any = {
    logWarning("Malformed record detected. Return default record")
    default
  }

  override def toString(): String = {
    s"new ${getClass.getName}(${default})";
  }

}

case class DeserializingDefaultRecordExceptionHandler(default: Any) extends DeSerExceptionHandler with Logging {

  def handle(exception: Throwable, deserializer: DelegatingAvroDeserializer, readerSchema: Schema): Any = {
    logWarning("Malformed record detected. Return default record")
    deserializer.deserialize(default)
  }

  override def toString(): String = {
    s"new ${getClass.getName}(${default})";
  }

}