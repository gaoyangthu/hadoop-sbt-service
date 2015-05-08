import AssemblyKeys._

assemblySettings

jarName in assembly := "hdfs-ftp.jar"

mainClass in assembly := Some("com.github.gaoyangthu.ftp.FtpService")

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp filter {_.data.getName == "activation-1.0.2.jar"}
}
