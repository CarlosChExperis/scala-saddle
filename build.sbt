ThisBuild / name := "pruebas-breeze"
ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.11.12"

//resolvers ++= Seq(
//  "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
//  "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
//)
//
//libraryDependencies ++= Seq(
//  "org.scala-saddle" %% "saddle-core" % "1.3.+"
//  // (OPTIONAL) "org.scala-saddle" %% "saddle-hdf5" % "1.3.+"
//)
//mainClass in assembly := Some("com.breeze.Example")
//
//assemblyJarName in assembly := "utils.jar"

lazy val root = (project in file("."))
  .settings(
    name := "root",
    assemblyJarName in assembly := "root.jar",
    resolvers ++= Seq(
      "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
    ),

    libraryDependencies ++= Seq(
      "org.scala-saddle" %% "saddle-core" % "1.3.+"
      // (OPTIONAL) "org.scala-saddle" %% "saddle-hdf5" % "1.3.+"
      ),
    mainClass in assembly := Some("com.breeze.Example")

  )