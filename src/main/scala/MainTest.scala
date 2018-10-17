import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream

object MainTest extends App {
  val spark = SparkSession.builder
    .appName("Spark Structured Streaming JDBC Sink Test App")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  implicit val ctx = spark.sqlContext

  val stream = MemoryStream[Int]
  stream.addData(1 to 1000)

  val q = stream
    .toDF()
    .select('value as "id", 'value * 123 as "testInt", lit("sss") as "testStr")
    .writeStream
    .format("org.apache.spark.sql.jdbcsink")
    .option("checkpointLocation", "checkpointLocation")
    .option("url", "jdbc:teradata://192.168.43.154/DATABASE=MM") //TYPE=FASTLOAD
    .option("dbtable", "mm.Test1")
    .option("user", "DBC")
    .option("password", "dbc")
    .start()

  q.awaitTermination()

}
