import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object FridExcre extends App{

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._


  val input = spark.range(50).withColumn("key", $"id" % 5)
  input.show()


  val solution = input.groupBy("key").agg(collect_set("id").as("all"))
    .withColumn("third", filter($"all", (x, idx) => idx <= 2)).show(false)


  val nums = Seq(Seq(1,2,3)).toDF("nums")

  val solution2 = nums.withColumn("num", explode($"nums"))
  solution2.show()


  val words = Seq(Array("hello", "world")).toDF("words")
  words.withColumn("words", concat_ws(",", $"words")).show(false)


  val dates = Seq(
    "08/11/2015",
    "09/11/2015",
    "09/12/2015").toDF("date_string")

  dates.withColumn("to_date", to_date($"date_string", "dd/MM/yyyy"))
    .withColumn("diff", datediff(current_date(), $"to_date")).show


  val data = Seq(
    (0, "2016-01-1"),
    (1, "2016-02-2"),
    (2, "2016-03-22"),
    (3, "2016-04-25"),
    (4, "2016-05-21"),
    (5, "2016-06-1"),
    (6, "2016-03-21")
  ).toDF("number_of_days", "date")

  data.withColumn("future", date_add($"date", $"number_of_days")).show

  def colToUpper(path: String, col: String) = {
    val df: DataFrame = spark.read.options(Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true")).csv(path)
    val listCols = col.split("\\W+").filter(df.schema(_).dataType.typeName == "string")
    df.select(df("*") +: listCols.map(col => upper(df(col)).as(s"upper_$col")) : _*)
  }

  colToUpper("cities.csv", "country,city")
}
