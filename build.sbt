import ReleaseTransformations._
import sbt.Keys.testForkedParallel

name := "ojai-testing"

scalaVersion := "2.11.8"


//lazy val supportedScalaVersions = Seq(scalaVersion., "2.12")

organization in ThisBuild := "com.github.anicolaspp"


lazy val maprdbconnector = project.in(file("."))
  .settings(

    homepage := Some(url("https://github.com/anicolaspp/ojai-testing")),

    scmInfo := Some(ScmInfo(url("https://github.com/anicolaspp/ojai-testing"), "git@github.com:anicolaspp/ojai-testing.git")),

    pomExtra := <developers>
      <developer>
        <name>Nicolas A Perez</name>
        <email>anicolaspp@gmail.com</email>
        <organization>anicolaspp</organization>
        <organizationUrl>https://github.com/anicolaspp</organizationUrl>
      </developer>
    </developers>,

    licenses += ("MIT License", url("https://opensource.org/licenses/MIT")),

    publishMavenStyle := true,

    publishTo := Some(Resolver.file("file", new File("/Users/nperez/.m2/repository"))),

    //    publishTo in ThisBuild := Some(
    //      if (isSnapshot.value)
    //        Opts.resolver.sonatypeSnapshots
    //      else
    //        Opts.resolver.sonatypeStaging
    //    ),

    publishArtifact in Test := false,

    parallelExecution in Test := false,

    testForkedParallel in Test := false,

    pomIncludeRepository := { _ => true },

    releasePublishArtifactsAction := PgpKeys.publishSigned.value,

    releaseCrossBuild := true,

    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies, // : ReleaseStep
      inquireVersions, // : ReleaseStep
      runClean, // : ReleaseStep
      runTest, // : ReleaseStep
      setReleaseVersion, // : ReleaseStep
      commitReleaseVersion, // : ReleaseStep, performs the initial git checks
      tagRelease, // : ReleaseStep
      publishArtifacts, // : ReleaseStep, checks whether `publishTo` is properly set up
      setNextVersion, // : ReleaseStep
      commitNextVersion, // : ReleaseStep
      pushChanges // : ReleaseStep, also checks that an upstream branch is properly configured
    ),

    resolvers += "MapR Releases" at "http://repository.mapr.com/maven/",

    resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",

    libraryDependencies ++= Seq(
      "com.mapr.ojai" % "mapr-ojai-driver" % "6.1.0-mapr",
      "org.apache.hadoop" % "hadoop-client" % "2.7.0-mapr-1808",
      "org.ojai" % "ojai" % "3.0-mapr-1808",
      "org.ojai" % "ojai-scala" % "3.0-mapr-1808",

      "com.mapr.db" % "maprdb" % "6.1.0-mapr",
      "xerces" % "xercesImpl" % "2.11.0",
      "org.scalactic" %% "scalactic" % "3.0.5",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test"
    )
  )

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)