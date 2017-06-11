package importer

import org.apache.spark.sql.hive.HiveContext

trait Importer {
	def doImport(hiveCtx: HiveContext, inputDir: String) : Unit
}