import sbt.Keys.libraryDependencies

name := "example-ethereum"
version := "0.5"
organization := "com.raphtory"

scalaVersion := "2.13.7"

resolvers += Resolver.mavenLocal
libraryDependencies += "com.raphtory" %% "core" % "0.5"
Compile / resourceDirectory := baseDirectory.value / "resources"

libraryDependencies ++= Seq(
        "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
        "com.typesafe"                % "config"        % "1.4.1"
)

resolvers += Resolver.mavenLocal


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _ @_*) => MergeStrategy.discard
  case _                           => MergeStrategy.first
}
