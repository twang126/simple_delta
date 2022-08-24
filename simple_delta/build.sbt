val scala3Version = "3.1.3"

lazy val root = project
  .in(file("."))
  .settings(
    name := "simple_delta",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      "org.scalatestplus" %% "mockito-3-4" % "3.2.10.0" % Test,
      "org.scalatest" %% "scalatest" % "3.2.12" % Test
    )
  )
