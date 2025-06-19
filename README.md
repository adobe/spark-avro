Avro for Spark

Supports Confluent Schema Registry and Apicurio Schema registry.

---

## Coordinates for dependency

```xml
<dependency>
    <groupId>com.adobe</groupId>
    <artifactId>spark-avro_2.12</artifactId>
    <version>1.0.0</version>
</dependency>
```

---

## Including

### Spark Shell

`spark-shell --packages com.adobe:spark-avro_2.12:1.0.0 --repositories https://packages.confluent.io/maven/`

### Maven 

```xml
<project>
    <repositories>
        <repository>
            <id>confluent</id>
            <url>https://packages.confluent.io/maven/</url>
        </repository>
    </repositories>

    <dependencies>
        <dependency>
            <groupId>com.adobe</groupId>
            <artifactId>spark-avro_2.12</artifactId>
            <version>1.0.0</version>
        </dependency>
    </dependencies>
</project>
```

---

# Usage

---
## Spark Scala

### Serialize

#### Serializing avro binary

```scala
import com.adobe.spark.sql.avro._
import com.adobe.spark.sql.avro.functions._

val schemaId = 1L
val registryConfig = Map(
  "schema.registry.url" -> "mock://registry", 
  "max.schemas.per.subject" -> "200", 
  "class" -> "com.adobe.spark.sql.avro.client.ApicurioRegistryClient"
)
val serializedColumn = to_avro(col("my_data"), serConfig(schemaId, registryConfig, writeSchemaId=true, magicByteSize=4), registryConfig)
```

#### Serializing avro json

```scala
import com.adobe.spark.sql.avro._
import com.adobe.spark.sql.avro.functions._

val schemaId = 1L
val registryConfig = Map(
  "schema.registry.url" -> "mock://registry", 
  "max.schemas.per.subject" -> "200", 
  "class" -> "com.adobe.spark.sql.avro.client.ApicurioRegistryClient"
)
val serializedColumn = to_avro_json(col("my_data"), serConfig(schemaId, registryConfig, magicByteSize=4), registryConfig) // writeSchemaId has no effect for json
```

#### Serializing avro binary using schema subject

```scala
import com.adobe.spark.sql.avro._
import com.adobe.spark.sql.avro.functions._

val schemaSubject = "my-schema"
val registryConfig = Map(
  "schema.registry.url" -> "mock://registry", 
  "max.schemas.per.subject" -> "200", 
  "class" -> "com.adobe.spark.sql.avro.client.ConfluentRegistryClient"
)
val serializedColumn = to_avro(col("my_data"), serConfigForSubject(schemaSubject, registryConfig, writeSchemaId=true, magicByteSize=4), registryConfig)
```

#### Serializing avro json using schema subject

```scala
import com.adobe.spark.sql.avro._
import com.adobe.spark.sql.avro.functions._

val schemaSubject = "my-schema"
val registryConfig = Map(
  "schema.registry.url" -> "mock://registry", 
  "max.schemas.per.subject" -> "200", 
  "class" -> "com.adobe.spark.sql.avro.client.ConfluentRegistryClient"
)
val serializedColumn = to_avro_json(col("my_data"), serConfigForSubject(schemaSubject, registryConfig, magicByteSize=4), registryConfig) // writeSchemaId has no effect for json
```

### Deserialize

#### Deserializing avro binary using schema id

```scala
import com.adobe.spark.sql.avro._
import com.adobe.spark.sql.avro.errors._
import com.adobe.spark.sql.avro.functions._

val schemaId = 1L
val registryConfig = Map(
  "schema.registry.url" -> "mock://registry", 
  "max.schemas.per.subject" -> "200", 
  "class" -> "com.adobe.spark.sql.avro.client.ConfluentRegistryClient"
)
val deserializerConfig = deSerConfig(schemaId, registryConfig,
  errOnEvolution = true, errHandler = FailFastExceptionHandler(), magicByteSize = 4) // These 3 are optional
val serializedColumn = from_avro(col("my_data"), deserializerConfig, registryConfig)
```

#### Deserializing avro binary using schema subject

```scala
import com.adobe.spark.sql.avro._
import com.adobe.spark.sql.avro.errors._
import com.adobe.spark.sql.avro.functions._

val schemaSubject = "my-schema"
val registryConfig = Map(
  "schema.registry.url" -> "mock://registry", 
  "max.schemas.per.subject" -> "200",  
  "class" -> "com.adobe.spark.sql.avro.client.ApicurioRegistryClient"
)
val deserializerConfig = deSerConfigForSubject(schemaSubject, registryConfig,
  errOnEvolution = true, errHandler = FailFastExceptionHandler(), magicByteSize = 4) // These 3 are optional
val serializedColumn = from_avro(col("my_data"), deserializerConfig, registryConfig)
```

#### Deserializing avro json

```scala
import com.adobe.spark.sql.avro._
import com.adobe.spark.sql.avro.errors._
import com.adobe.spark.sql.avro.functions._

val schemaId = 1L
val registryConfig = Map(
  "schema.registry.url" -> "mock://registry", 
  "max.schemas.per.subject" -> "200", 
  "class" -> "com.adobe.spark.sql.avro.client.ConfluentRegistryClient"
)
val deserializerConfig = deSerConfig(schemaId, registryConfig, 
  errOnEvolution = true, errHandler = FailFastExceptionHandler(), magicByteSize = 4) // These 3 are optional
val serializedColumn = from_avro_json(col("my_data"), col("writerSchemaId"), deserializerConfig, registryConfig)
```

---

## Spark-SQL/functions.expr

### Register function:

```scala
com.adobe.spark.sql.avro.functions.registerFunctions()
```

### Deserialize

#### Deserializing avro binary using writer schema id in magic byte

```sql
SELECT 
  from_avro_binary(
   data, 
   map(
     'schemaId', 1, 
     'errOnEvolution', 'false', 
     'errHandler', 'com.adobe.spark.sql.avro.errors.DefaultRecordExceptionHandler', 
     'magicByteSize', '4'
   ), 
   'default-value', 
   map('schema.registry.url', 'mock://registry', 'max.schemas.per.subject', '200')
  ) AS value
FROM dataset
""".stripMargin

```
#### Deserializing avro binary using writer schema subject

```sql
SELECT 
  from_avro_binary(
  data, 
   map(
     'subject', 'deserialize-string-correctly', 
     'errOnEvolution', 'false', 
     'errHandler', 'com.adobe.spark.sql.avro.errors.DefaultRecordExceptionHandler', 
     'magicByteSize', '4'
   ), 
   'default-value', 
   map('schema.registry.url', 'mock://registry', 'max.schemas.per.subject', '200')
  ) AS value
FROM dataset
""".stripMargin
```

#### Deserializing avro binary using writer schema id in column
                  
```sql
SELECT 
  from_avro_binary_with_id(
   data, 
   schemaId, 
   map(
     'subject', 'deserialize-string-correctly', 
     'errOnEvolution', 'false', 
     'errHandler', 'com.adobe.spark.sql.avro.errors.DefaultRecordExceptionHandler', 
     'magicByteSize', '4'
   ), 
   'HELLO', 
   map('schema.registry.url', 'mock://registry', 'max.schemas.per.subject', '200')
  ) AS value
FROM dataset
```

#### Deserializing avro json using writer schema id in column

```sql
SELECT
  from_avro_json(
   data, 
   schemaId, 
   map(
     'subject', 'deserialize-string-correctly', 
     'errOnEvolution', 'false', 
     'errHandler', 'com.adobe.spark.sql.avro.errors.DefaultRecordExceptionHandler', 
     'magicByteSize', '4'
   ), 
   'default-value', 
   map('schema.registry.url', 'mock://registry', 'max.schemas.per.subject', '200')
  ) AS value
FROM dataset
""".stripMargin
```

#### Serializing avro binary

```sql
SELECT 
  to_avro_binary(
   data, 
   map(
     'subject', 'serialize-struct-correctly', 
     'writeSchemaId', 'true', 
     'magicByteSize', '4'
   ), 
   'HELLO', 
   map('schema.registry.url', 'mock://registry', 'max.schemas.per.subject', '200')
  ) AS record 
FROM dataset
```

#### Serializing avro json

```sql
SELECT 
  to_avro_json(
   record, 
   map(
     'subject', 'serialize-struct-correctly', 
     'writeSchemaId', 'true', 
     'magicByteSize', '4'
   ), 
   'HELLO', 
   map('schema.registry.url', 'mock://registry', 'max.schemas.per.subject', '200')
  ) AS record
FROM dataset
```



## Choosing schema registry:

### Apicurio

```scala
val registryConfig = Map(
  "schema.registry.url" -> "mock://registry", // Replace with your registry endpoint
  "max.schemas.per.subject" -> "200",
  "class" -> "com.adobe.spark.sql.avro.client.ApicurioRegistryClient"
)
```

```sql
   map('schema.registry.url', 'mock://registry')  /* Replace with your registry endpoint*/
```

```sql
   map('schema.registry.url', 'mock://registry', 'class', 'com.adobe.spark.sql.avro.client.ApicurioRegistryClient') /* Replace with your registry endpoint*/
```


### Confluent

```scala
val registryConfig = Map(
    "schema.registry.url" -> "mock://registry", // Replace with your registry endpoint
    "max.schemas.per.subject" -> "200",
    "class" -> "com.adobe.spark.sql.avro.client.ConfluentRegistryClient"
)
```

```sql
   map('schema.registry.url', 'mock://registry', 'class', 'com.adobe.spark.sql.avro.client.ConfluentRegistryClient') /* Replace with your registry endpoint*/
```


## Deserialization Error handling:

You can handle errors when deserializing records using one of the `DeSerExceptionHandler`.
Refer above samples for example.

### FailFastExceptionHandler
Fails the job by throwing `new SparkException("Malformed record detected. Fail fast.", exception)`

### PermissiveRecordExceptionHandler
Returns a record matching the passed schema with all fields as null

### NullReturningRecordExceptionHandler
Returns null

### DefaultRecordExceptionHandler
Returns the default value as provided

### DeserializingDefaultRecordExceptionHandler
Returns the value after deserializing the default value against the reader schema