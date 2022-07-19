name := "aws"
version := "0.1.0"
organization := "com.raphtory"
scalaVersion := "2.13.7"
libraryDependencies += "com.raphtory" %% "core"             % "0.1.0"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3"  % "1.12.239"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-sts" % "1.12.239"
resolvers += Resolver.mavenLocal
