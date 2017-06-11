package importer.udf

import org.apache.spark.sql.functions._

object functions {

	val dayOfWeek = udf[String,String]((fdate:String) => {
	    import java.text.SimpleDateFormat
	    val df = new SimpleDateFormat("yyyy-MM-dd")
	    new SimpleDateFormat("EEEEEE").format(df.parse(fdate))
	})

	val monthName = udf[String,Int]((month:Int) => {
	    month match {
	        case 1 => "January"
	        case 2 => "Febuary"
	        case 3 => "March"
	        case 4 => "April"
	        case 5 => "May"
	        case 6 => "June"
	        case 7 => "July"
	        case 8 => "August"
	        case 9 => "September"
	        case 10 => "October"
	        case 11 => "November"
	        case 12 => "December"
	        case _ => "Unknown"
	    }
	})
}