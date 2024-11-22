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
inThisBuild(List(
  organization := "com.adobe",
  homepage := Some(url("https://github.com/adobe/spark-avro")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      "Adobe",
      "Adobe",
      "repo@adobe.com",
      url("http://www.adobe.com")
    )
  )
))

ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.19"

lazy val root = (project in file("."))
  .settings(
    organization := "com.adobe", 
    name := "spark-avro"
  )

lazy val sparkVersion = "3.5.1" // Use the desired Spark version

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided", 
  "org.apache.spark" %% "spark-avro" % sparkVersion % "provided"
)

libraryDependencies ++= Seq(
  "io.apicurio" % "apicurio-registry-client" % "3.0.0.M2",
  "io.confluent" % "kafka-schema-registry-client" % "6.2.1"
)

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.19"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % "test"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.1.0" % "test"

resolvers += "Apache Spark Repository" at "https://repository.apache.org/snapshots/"
resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"
resolvers += "Confluent" at "https://packages.confluent.io/maven/"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
