val Versions = new {
  val Aggregator = "0.3.1-SNAPSHOT"
  val Scala      = "2.13.10"
}

// set the version of scala to compile with
scalaVersion := Versions.Scala

// add scala compile flags
scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-Ywarn-value-discard"
)

// add required libraries
libraryDependencies ++= Seq(
  "org.broadinstitute.dig" %% "dig-aggregator-core" % Versions.Aggregator
)

// set the oranization this method belongs to
organization := "org.broadinstitute.dig"

// entry point when running this method
mainClass := Some("org.broadinstitute.dig.aggregator.methods.nearestgene.GeneIdMap")

// enables buildInfo, which bakes git version info into the jar
enablePlugins(GitVersioning)

// get the buildInfo task
val buildInfoTask = taskKey[Seq[File]]("buildInfo")

// define execution code for task
buildInfoTask := {
  val file = (Compile / resourceManaged).value / "version.properties"

  // log where the properties will be written to
  streams.value.log.info(s"Writing version info to $file...")

  // collect git versioning information
  val branch                = git.gitCurrentBranch.value
  val lastCommit            = git.gitHeadCommit.value
  val describedVersion      = git.gitDescribedVersion.value
  val anyUncommittedChanges = git.gitUncommittedChanges.value
  val remoteUrl             = (ThisBuild / scmInfo).value.map(_.browseUrl.toString)
  val buildDate             = java.time.Instant.now

  // map properties
  val properties = Map[String, String](
    "branch"             -> branch,
    "lastCommit"         -> lastCommit.getOrElse(""),
    "remoteUrl"          -> remoteUrl.getOrElse(""),
    "uncommittedChanges" -> anyUncommittedChanges.toString,
    "buildDate"          -> buildDate.toString
  )

  // build properties content
  val contents = properties.toList.map {
    case (key, value) if value.nonEmpty => s"$key=$value"
    case _                                => ""
  }

  // output the version information from git to versionInfo.properties
  IO.write(file, contents.mkString("\n"))
  Seq(file)
}

// add the build info task output to resources
(Compile / resourceGenerators) += buildInfoTask.taskValue
