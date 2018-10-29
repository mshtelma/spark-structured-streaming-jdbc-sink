import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

object MainTestKafka extends App {
  val spark = SparkSession.builder
    .appName("Spark Structured Streaming JDBC Sink Kafka Test App")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .master("local[8]")
    .getOrCreate()

  implicit val ctx = spark.sqlContext

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .load()

  val q = df.select(df("value").cast(StringType).as("data"))
    .writeStream
    .format("org.apache.spark.sql.jdbcsink")
    .option("checkpointLocation", "checkpointLocation")
    .option("url", "jdbc:teradata://192.168.1.102/DATABASE=TESTDB,TYPE=FASTLOAD")
    .option("dbtable", "testdb.testTable")
    .option("user", "DBC")
    .option("password", "dbc")
    .start()

  q.awaitTermination()
}
