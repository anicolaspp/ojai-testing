import ReleaseTransformations._
name := "ojai-testing"

scalaVersion := "2.12.8"


//lazy val supportedScalaVersions = Seq(scalaVersion., "2.12")

organization in ThisBuild := "com.github.anicolaspp"


lazy val maprdbconnector = project.in(file("."))
  .settings(

    homepage := Some(url("https://github.com/anicolaspp/MapRDBConnector")),

    scmInfo := Some(ScmInfo(url("https://github.com/anicolaspp/MapRDBConnector"), "git@github.com:anicolaspp/MapRDBConnector.git")),

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

    publishTo := Some(Resolver.file("file", new File("/Users/nperez/maven"))),
    //
    //    publishTo in ThisBuild := Some(
    //      if (isSnapshot.value)
    //        Opts.resolver.sonatypeSnapshots
    //      else
    //        Opts.resolver.sonatypeStaging
    //    ),

    publishArtifact in Test := false,

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

    //    crossScalaVersions := supportedScalaVersions,

    resolvers += "MapR Releases" at "http://repository.mapr.com/maven/",

    resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",

    libraryDependencies ++= Seq(
      "org.ojai" % "ojai" % "3.0-mapr-1808",
      "org.ojai" % "ojai-scala" % "3.0-mapr-1808",

      "com.mapr.db" % "maprdb" % "6.1.0-mapr" % "provided",
      "xerces" % "xercesImpl" % "2.11.0" % "provided",
      "org.scalactic" %% "scalactic" % "3.0.5",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test"
    )
  )

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", xs@_*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyJarName := s"${name.value}-${version.value}.jar"
