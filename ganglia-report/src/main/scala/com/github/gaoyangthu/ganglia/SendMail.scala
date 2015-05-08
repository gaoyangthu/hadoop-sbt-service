package com.github.gaoyangthu.ganglia

/**
 * Created by GaoYang on 2014/5/21 0021.
 */

import com.github.gaoyangthu.ganglia.mail._
import com.github.gaoyangthu.ganglia.statistics._

object report {

	def main (args: Array[String]) {
		send a new Mail (
			hostName = "smtp.gmail.com",
			userName = "xxx",
			password = "xxx",
			from = ("xxx@gmail.com", "Big Data Product Line"),
			to = List("xxx@gmail.com", "xxx@gmail.com"),
			subject = "Ganglia Report",
			message = getYesterdayStatistics
		)
		println("Send a email!")
	}
}
