name := "ganglia-report"

organization := "gaoyangthu.github.com"

version := "1.0"

scalaVersion := "2.11.0"

libraryDependencies += "commons-email" % "commons-email" % "1.0"

libraryDependencies += "info.ganglia.gmetric4j" % "gmetric4j" % "1.0.7"

resolvers ++= Seq(
	"Private CDH Repository" at "http://172.16.0.6:8081/nexus/content/groups/public/",
	"OSChina Repository" at "http://maven.oschina.net/service/local/repositories/central/content/",
	"JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
	"Spray Repository" at "http://repo.spray.cc/",
	"Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
	"Akka Repository" at "http://repo.akka.io/releases/"
)
