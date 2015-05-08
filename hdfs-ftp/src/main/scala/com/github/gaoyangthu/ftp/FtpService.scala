package com.github.gaoyangthu.ftp

import com.github.gaoyangthu.ftp._

import java.io._
import java.net.{ServerSocket, Socket, InetSocketAddress}
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.concurrent.Executors.newCachedThreadPool

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.jboss.netty.handler.execution.{ExecutionHandler, OrderedMemoryAwareThreadPoolExecutor}
import org.jboss.netty.handler.timeout.{WriteTimeoutHandler, ReadTimeoutHandler}
import org.jboss.netty.util.{Timer, HashedWheelTimer}

object FtpService {
	class PipelineFactory extends ChannelPipelineFactory {

		var inets: InetSocketAddress = null
		var inetsAdvertised: InetSocketAddress = null

		var lowestPassivePort: Int = -1
		var highestPassivePort: Int = -1

		val timer: Timer = new HashedWheelTimer

		override def getPipeline: ChannelPipeline = {
			println("A client has been connected!");
			val pipe = Channels.pipeline();
			//pipe.addLast("decoder", new CrlfStringDecoder());
			pipe.addLast("encoder", new StringEncoder(Charset.forName("US-ASCII")));
			pipe.addLast("decoder", new StringDecoder(Charset.forName("US-ASCII")));
			pipe.addLast("readTimeout", new ReadTimeoutHandler(timer, 30))
			pipe.addLast("writeTimeout", new WriteTimeoutHandler(timer, 30))
			//pipe.addLast("executor", new ExecutionHandler(new OrderedMemoryAwareThreadPoolExecutor(100, 0, 0)));
			pipe.addLast("executor", SharedResource.getExecutionHandler);
			//pipe.addLast("handler", new FtpServerHandler(new ConsoleReceiver, inets.getAddress, inetsAdvertised.getAddress, lowestPassivePort, highestPassivePort, 10, 0));
			pipe.addLast("handler", new FtpServerHandler(new ConsoleReceiver, inets.getAddress, inetsAdvertised.getAddress, lowestPassivePort, highestPassivePort, 10, 0){
				override def list(ctx: ChannelHandlerContext, args: String) = {
					var hdfs: FileSystem = SharedResource.getHdfs
					val sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm")
					var dir: String = null
					if (args.length == 0 || args == null || args.equals("")) {
						dir = hdfsDir.get
					} else {
						if (hdfsDir.get.endsWith("/")) {
							dir = hdfsDir.get + args
						} else {
							dir = hdfsDir.get + "/" + args
						}
					}
					if ("PORT" equals lastCommand.get) {
						val as: Socket = activeSocket.get
						if (null != as) {
							send("150 Here comes the directory listing." + args, ctx, "LIST", args)
							try {
								for (fs <- hdfs.listStatus(new Path(dir))) {
									var sb: StringBuilder = new StringBuilder("")
									if (fs.isDirectory) {
										sb.append("d").append(fs.getPermission).append(" ")
									} else if (fs.isFile) {
										sb.append("-").append(fs.getPermission).append(" ")
									}
									sb.append(fs.getReplication).append(" ")
									sb.append("%-10s".format(fs.getOwner)).append(" ")
									sb.append("%-10s".format(fs.getGroup)).append(" ")
									sb.append("%-15d".format(fs.getLen)).append(" ")
									sb.append(sdf.format(fs.getModificationTime)).append(" ")
									sb.append(fs.getPath.getName).append("\r\n")
									as.getOutputStream.write(sb.toString.getBytes())
								}
								send("226 Directory send OK.", ctx, "", args)
							} catch {
								case e: IOException => send("552 Requested file action aborted", ctx, "LIST", args);
							} finally {
								super.closeActiveSocket(as)
							}
						} else {
							super.send("503 Bad sequence of commands", ctx, "STOR", args)
						}
					} else if ("PASV" equals lastCommand.get) {
						val ps: ServerSocket = passiveSocket.get
						if (null != ps) {
							send("150 Here comes the directory listing." + args, ctx, "LIST", args)
							var clientSocket: Socket = null
							try {
								clientSocket = ps.accept()
								for (fs <- hdfs.listStatus(new Path(dir))) {
									var sb: StringBuilder = new StringBuilder("")
									if (fs.isDirectory) {
										sb.append("d").append(fs.getPermission).append(" ")
									} else if (fs.isFile) {
										sb.append("-").append(fs.getPermission).append(" ")
									}
									sb.append(fs.getReplication).append(" ")
									sb.append("%-10s".format(fs.getOwner)).append(" ")
									sb.append("%-10s".format(fs.getGroup)).append(" ")
									sb.append("%-15d".format(fs.getLen)).append(" ")
									sb.append(sdf.format(fs.getModificationTime)).append(" ")
									sb.append(fs.getPath.getName).append("\r\n")
									clientSocket.getOutputStream.write(sb.toString.getBytes())
								}
								clientSocket.getOutputStream().close()
								send("226 Directory send OK.", ctx, "", args);
							} catch {
								case e: IOException => { clientSocket.getOutputStream().close(); send("552 Requested file action aborted", ctx, "LIST", args); }
							} finally {
								super.closePassiveSocket(ps)
							}
						} else {
							super.send("503 Bad sequence of commands", ctx, "STOR", args)
						}
					} else {
						super.send("503 Bad sequence of commands", ctx, "LIST", args)
					}
				}
				override def retr(ctx: ChannelHandlerContext, args: String) = {
					var hdfs: FileSystem = SharedResource.getHdfs
					if ("PORT" equals lastCommand.get) {
						val as: Socket = activeSocket.get
						if (null != as) {
							send("150 Opening binary mode data connection for " + args, ctx, "RETR", args)
							var is: InputStream = null
							try {
								val file: String = if (hdfsDir.get.endsWith("/")) hdfsDir.get+args else hdfsDir.get+"/"+args
								is = hdfs.open(new Path(file))
								IOUtils.copy(is, as.getOutputStream)
									send("226 File send OK.", ctx, "", args)
							} catch {
								case e: IOException => send("552 Requested file action aborted", ctx, "LIST", args);
							} finally {
								is.close
								super.closeActiveSocket(as)
							}
						} else send("503 Bad sequence of commands", ctx, "RETR", args)
					} else if ("PASV" equals lastCommand.get) {
						val ps: ServerSocket = passiveSocket.get
						if (null != ps) {
							send("150 Opening binary mode data connection for " + args, ctx, "RETR", args)
							var clientSocket: Socket = null
							var is: InputStream = null
							try {
								clientSocket = ps.accept()
								val file: String = if (hdfsDir.get.endsWith("/")) hdfsDir.get+args else hdfsDir.get+"/"+args
								is = hdfs.open(new Path(file))
								IOUtils.copy(is, clientSocket.getOutputStream)
								clientSocket.getOutputStream.flush
								clientSocket.getOutputStream.close
								send("226 File send OK.", ctx, "", args);
							} catch {
								case e: IOException => { clientSocket.getOutputStream().close(); send("552 Requested file action aborted", ctx, "LIST", args); }
							} finally {
								is.close
								super.closePassiveSocket(ps)
							}
						} else send("503 Bad sequence of commands", ctx, "RETR", args)
					} else send("503 Bad sequence of commands", ctx, "RETR", args)
				}
			});
			pipe;
		}
	}

	class ConsoleReceiver extends DataReceiver {
		var hdfs: FileSystem = SharedResource.getHdfs

		override def receive(directory: String, name: String, data: InputStream) {
			var start = System.currentTimeMillis

			var outputStream: OutputStream = null

			try {
				val path = new Path(directory)
				var file: Path = null
				if (directory.endsWith("/")) {
					file = new Path(directory + name)
				} else {
					file = new Path(directory + "/" + name)
				}

				hdfs.mkdirs(path)
				outputStream = hdfs.create(file)

				val file_size = IOUtils.copy(data, outputStream);

				val end = System.currentTimeMillis
				val speed = file_size * 8 / (end - start) / 1024.0

				printf("[%d|%d|%d|%.2f] %s \n ", start, end, file_size, speed, file)

			} catch {
				case e: Exception => e.printStackTrace();
			} finally {
				if (outputStream != null) {
					outputStream.flush
					outputStream.close
				}
			}
		}
	}

	def main(args: Array[String]) {
		val factory = new NioServerSocketChannelFactory(newCachedThreadPool(), newCachedThreadPool());
		val bootstrap = new ServerBootstrap(factory);

		val pipeline = new PipelineFactory

		var inets = new InetSocketAddress(args(0), args(2).toInt)
		var inetsAdvertised = new InetSocketAddress(args(1), args(2).toInt)

		pipeline.lowestPassivePort = args(3).toInt
		pipeline.highestPassivePort = args(4).toInt

		pipeline.inets = inets
		pipeline.inetsAdvertised = inetsAdvertised

		bootstrap.setPipelineFactory(pipeline);
		bootstrap.bind(inets);
	}
}

object SharedResource {
	var hdfs: FileSystem = null
	var threadPool: OrderedMemoryAwareThreadPoolExecutor = null
	var execution: ExecutionHandler = null

	def getHdfs: FileSystem = {
		if (hdfs == null) {
			val conf = new Configuration()
			val conf_core = new FileInputStream("/etc/hadoop/conf/core-site.xml")
			val conf_hdfs = new FileInputStream("/etc/hadoop/conf/hdfs-site.xml")
			conf.addResource(conf_core)
			conf.addResource(conf_hdfs)
			hdfs = FileSystem.get(conf)
			conf_core.close
			conf_hdfs.close
		}
		hdfs
	}

	def getThreadPool: OrderedMemoryAwareThreadPoolExecutor = {
		if (threadPool == null) {
			threadPool = new OrderedMemoryAwareThreadPoolExecutor(1000, 0, 0)
		}
		threadPool
	}

	def getExecutionHandler: ExecutionHandler = {
		if (execution == null) {
			execution = new ExecutionHandler(SharedResource.getThreadPool)
		}
		execution
	}
}
