import sqlContext._
import sqlContext.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class BTable(bid:String, addr:String, cat:String)
case class rTable(rid:String, rat:Double)

object Q1 {
	def exec() {
		val business = sc.textFile("hdfs:///yelpdatafall/business/business.csv")
		val review = sc.textFile("hdfs:///yelpdatafall/review/review.csv")
	
		val btabDF = business.map(_.split('^')).map(b => BTable(b(0), b(1), b(2))).toDF()
		val rtabDF = review.map(_.split('^')).map(r => rTable(r(2), r(3).toDouble)).toDF()

		btabDF.registerTempTable("bus")
		rtabDF.registerTempTable("rev")

		val joined = sqlContext.sql("""
		SELECT b.bid, b.addr, b.cat, avg(r.rat) as avgRat 
		FROM bus b JOIN rev r
		ON b.bid = r.rid group by b.bid, b.addr, b.cat order by avgRat desc limit 10""")

		joined.rdd.saveAsTextFile("hdfs:///user/ass150430/q1_out")
		System.exit(0)
	}
}
Q1.exec()