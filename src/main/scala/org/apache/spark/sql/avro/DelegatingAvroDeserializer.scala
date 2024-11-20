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

package org.apache.spark.sql.avro

import org.apache.avro.Schema
import org.apache.spark.sql.types.DataType;


/*
  Wrap the spark avro deserializers so we can use same interface across versions.
*/
class DelegatingAvroDeserializer(schema: Schema, catalystType: DataType) {
  private val delegate: AvroDeserializer = {
    val klass = classOf[AvroDeserializer]
    val schemaKlass = classOf[Schema]
    val dataTypeKlass = classOf[DataType]
    val stringTypeKlass = classOf[String]
    val booleanKlass = classOf[Boolean]
    
    // Handle constructors for older versions of spark. For internal use
    klass.getConstructors.collectFirst {
      case constructor if constructor.getParameterTypes sameElements
        Array(schemaKlass, dataTypeKlass, stringTypeKlass, booleanKlass) =>
        constructor.newInstance(schema, catalystType, "LEGACY", false: java.lang.Boolean)
      case constructor if constructor.getParameterTypes.toSeq sameElements
        Array(schemaKlass, dataTypeKlass, stringTypeKlass, booleanKlass, stringTypeKlass) =>
        constructor.newInstance(schema, catalystType, "LEGACY", false: java.lang.Boolean, "")
    } match {
      case Some(value: AvroDeserializer) =>
        value
      case _ =>
        throw new NoSuchMethodException(
          s"""Supported constructors for AvroDeserializer are:
             |${klass.getConstructors.toSeq.mkString(System.lineSeparator())}""".stripMargin)
    }
    
  }

  def deserialize(data: Any): Any = {
    delegate.deserialize(data) match {
      case Some(x) => x
      case x => x
    }
  }
  
}
