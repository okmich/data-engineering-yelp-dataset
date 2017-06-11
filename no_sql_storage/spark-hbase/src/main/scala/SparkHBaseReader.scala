

object SparkHBaseReader {

	def main(args : Array[String]) : Unit = {
		val opts = parseCmdLineArgs(args)

		val sparkConf = new SparkConf().setAppName("Spark HBase Reader")
		sparkConf.set("hbase.zookeeper.quorum", "localhost")
		sparkConf.set("hbase.zookeeper.property.clientPort", "2181")
		sparkConf.set("spark.hbase.host", "localhost")

		val sparkContext  = new SparkContext(sparkConf)
	}
}