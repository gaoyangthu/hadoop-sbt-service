import sbt._
import Keys._

object SbtProjectBuild extends Build {
	lazy val root = Project(id = "root", base = file(".")) aggregate(gangliaReport, hdfsFtp)

	lazy val gangliaReport = Project(id = "ganglia-report", base = file("ganglia-report"))

	lazy val hdfsFtp = Project(id = "hdfs-ftp", base = file("hdfs-ftp"))
}