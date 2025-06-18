package com.adobe.spark.sql.avro.errors

case class InvalidSchemaIdError(id: Long, cause: Throwable = None.orNull) extends RuntimeException(s"Invalid schema id ${id}", cause) with Serializable {

}

case class RegistryCallError(message: String, cause: Throwable = None.orNull) extends RuntimeException(message, cause) with Serializable {

}
