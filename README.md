Avro for Spark

---

## Coordinates for dependency

```xml
<dependency>
    <groupId>com.adobe</groupId>
    <artifactId>spark-avro</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

---


## Usage

### Deserialize


#### Using Schema Id

```scala
import com.adobe.spark.sql.avro._
val schemaId = 1L
val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
val serializedColumn = to_avro(col("my_data"), serConfig(schemaId, registryConfig), registryConfig)
```

#### Using Schema Subject

```scala
import com.adobe.spark.sql.avro._
val schemaSubject = "my-schema"
val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
val serializedColumn = to_avro(col("my_data"), serConfigForSubject(schemaSubject, registryConfig), registryConfig)
```

### Serialize

#### Using Schema Id

```scala
import com.adobe.spark.sql.avro._
import com.adobe.spark.sql.avro.errors._

val schemaId = 1L
val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
val deserializerConfig = deSerConfig(schemaId, registryConfig, 
  errOnEvolution = true, errHandler = FailFastExceptionHandler(), magicByteSize = 4) // These 3 are optional
val serializedColumn = from_avro(col("my_data"), deserializerConfig, registryConfig)
```

#### Using Schema Subject

```scala
import com.adobe.spark.sql.avro._
import com.adobe.spark.sql.avro.errors._
val schemaSubject = "my-schema"
val registryConfig = Map("schema.registry.url" -> "mock://registry", "max.schemas.per.subject" -> "200")
val deserializerConfig = deSerConfigForSubject(schemaSubject, registryConfig,
  errOnEvolution = true, errHandler = FailFastExceptionHandler(), magicByteSize = 4) // These 3 are optional
val serializedColumn = from_avro(col("my_data"), deserializerConfig, registryConfig)
```