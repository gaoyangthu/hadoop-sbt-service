import AssemblyKeys._

assemblySettings

jarName in assembly := "ganglia-report.jar"

mainClass in assembly := Some("com.github.gaoyangthu.ganglia.report")

excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
  cp filter {_.data.getName == "activation-1.0.2.jar"}
}
