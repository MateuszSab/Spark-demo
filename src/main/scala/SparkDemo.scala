import org.apache.spark.sql.DataFrame

object SparkDemo extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()


  val df: DataFrame = spark.read.text("C:\\Users\\m.sabatowski\\IdeaProjects\\Spark-demo\\build.sbt")

  df.show(truncate = false)

  df.write.text("kv")
  spark.read.text("kv").show()
}
