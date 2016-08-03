import sqlContext._
import sqlContext.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class Table1(uid:String, name:String)
case class Table2(ruid:String, rat:Double)

object Q4 {
	def exec() {
		sc.setLogLevel("WARN")
		val user = sc.textFile("/FileStore/tables/eowuz3sy1467807212002/user.csv")
		val review = sc.textFile("/FileStore/tables/eowuz3sy1467807212002/review.csv")

		val DF1 = user.map(_.split('^')).map(b => Table1(b(0), b(1))).toDF()
		val DF2 = review.map(_.split('^')).map(r => Table2(r(1), r(3).toDouble)).toDF()

		DF1.registerTempTable("user")
		DF2.registerTempTable("rev")

		val joined = sqlContext.sql("""
		SELECT u.uid, u.name
		FROM user u JOIN rev r
		ON u.uid = r.ruid group by u.uid, u.name order by count(*) desc limit 10""")

		joined.collect.foreach(println)
		System.exit(0)
	}
}
Q4.exec()