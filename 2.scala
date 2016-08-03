import sqlContext._
import sqlContext.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class Table1(uid:String, name:String)
case class Table2(ruid:String, rat:Double)

object Q2 {
	def exec() {
		sc.setLogLevel("WARN")

		val user = sc.textFile("hdfs:///yelpdatafall/user/user.csv")
		val review = sc.textFile("hdfs:///yelpdatafall/review/review.csv")

		val DF1 = user.map(_.split('^')).map(u => Table1(u(0), u(1))).toDF()
		val DF2 = review.map(_.split('^')).map(r => Table2(r(1), r(3).toDouble)).toDF()

		DF1.registerTempTable("user")
		DF2.registerTempTable("rev")

		val joined = sqlContext.sql(s"""
		SELECT u.name, avg(r.rat) as avg_Rating
		FROM (SELECT user.uid, user.name from user where name LIKE '%Leesha Z.%') u JOIN rev r
		ON u.uid = r.ruid group by u.uid, u.name""")

		joined.collect.foreach(println)
		System.exit(0)
	}
}
Q2.exec()

