import sqlContext._
import sqlContext.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class BTable(bid:String, addr:String)
case class rTable(uid:String, rid:String, rat:Double)

object Q3 {
	def exec() {
		sc.setLogLevel("WARN")
		val business = sc.textFile("/FileStore/tables/eowuz3sy1467807212002/business.csv")
		val review = sc.textFile("/FileStore/tables/eowuz3sy1467807212002/review.csv")

		val btabDF = business.map(_.split('^')).map(b => BTable(b(0), b(1))).toDF()
		val rtabDF = review.map(_.split('^')).map(r => rTable(r(1), r(2), r(3).toDouble)).toDF()

		btabDF.registerTempTable("bus")
		rtabDF.registerTempTable("rev")

		val joined = sqlContext.sql("""
		SELECT r.uid,r.rat 
		FROM (SELECT distinct b.bid as bid FROM bus b where b.addr LIKE '%Stanford,%') btemp JOIN rev r
		ON btemp.bid = r.rid""")

		joined.collect.foreach(println)
		System.exit(0)
	}
}
Q3.exec()