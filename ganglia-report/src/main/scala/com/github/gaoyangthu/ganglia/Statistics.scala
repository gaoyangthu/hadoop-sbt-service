package com.github.gaoyangthu.ganglia

/**
 * Created by GaoYang on 2014/5/22 0022.
 */

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.immutable.HashMap
import scala.io.Source
import scala.math._

object statistics {
	def getYesterdayStatistics(): String = {
		val array = Array("ChannelCapacity", "ChannelFillPercentage","EventPutAttemptCount", "EventPutSuccessCount", "EventTakeAttemptCount", "EventTakeSuccessCount")
		val set = HashMap("ChannelCapacity"->1, "ChannelFillPercentage"->1, "EventPutAttemptCount"->1, "EventPutSuccessCount"->1, "EventTakeAttemptCount"->1, "EventTakeSuccessCount"->1)

		var map = new HashMap[String, HashMap[String, String]]

		var finalResult: String = ""

		val dir = new File("/tmp/ganglia/rrd/").listFiles().filter(_.isFile)
		for (file <- dir) {
			val channel = file.getName.split("\\.")(0) + ": " + file.getName.split("\\.")(1)
			val catagory = file.getName.split("\\.")(2)

			var title: String = "|%-23s|".format(channel)

			if (set contains catagory) {
				val lines = Source.fromFile(file).getLines.toArray

				val sdf = new SimpleDateFormat("HH:mm")

				var content: String = "|%-23s|".format(catagory)

				var flag: Int = 1
				var start: Double = 0
				var end: Double = 0

				for (line <- lines) {
					val date = new Date(line.split( """:""")(0).toLong * 1000)
					title += "%-6s|".format(sdf.format(date))

					var value: Double = 0
					
					if (!line.split( """ """)(1).equals("-nan")) {
						value = line.split( """ """)(1).split( """e""")(0).toDouble * pow(10, line.split( """ """)(1).split( """e""")(1).toDouble)
					}

					if (flag == 1) {
						start = value
					} else if (flag == lines.length) {
						end = value
					}
					flag += 1

					content += "%-6s|".format(value)
				}
				title += "%-6s|".format("Diff")
				content += "%-6s|".format((end - start))

				if (map contains channel) {
					var tmp: HashMap[String, String] = map(channel)
					tmp += (catagory -> content)
					map += (channel -> tmp)
				} else {
					map += (channel -> HashMap("Title" -> title, catagory -> content))
				}
			}
		}

		for ((key, value) <- map) {
			finalResult += value("Title") + "\n"
			for (a <- array) {
				finalResult += value(a) + "\n"
			}
			finalResult += "\n"
		}
		finalResult
	}

//	def main(args: Array[String]) {
//		print(getYesterdayStatistics())
//	}
}
