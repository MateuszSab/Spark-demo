import org.apache.spark.sql.functions._

object excercise12_04 extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val input = Seq(
    (1, "Mr"),
    (1, "Mme"),
    (1, "Mr"),
    (1, null),
    (1, null),
    (1, null),
    (2, "Mr"),
    (3, null),
    (3, "trrrrrrrrrr"),
    (3, "t"),
    (3, "t")).toDF("UNIQUE_GUEST_ID", "PREFIX")

//  val solution = input.groupBy($"UNIQUE_GUEST_ID").agg(greatest($"PREFIX")).show

  import org.apache.spark.sql.expressions._
  def windowSpec = Window.partitionBy("UNIQUE_GUEST_ID", "PREFIX")

  input.withColumn("count", count("PREFIX").over(windowSpec))
    .orderBy($"count".desc).groupBy("UNIQUE_GUEST_ID")
    .agg(first("PREFIX")
      .as("value")).show


  val data = Seq(
    (None, 0),
    (None, 1),
    (Some(2), 0),
    (None, 1),
    (Some(4), 1)).toDF("id", "group")

  data.groupBy($"group").agg(first($"id", ignoreNulls = true).as("first_non_null")).show

  val table = Seq(
    (1, "VPV"),
    (1, "MV1"),
    (1, "MV2"),
    (2, "VPV"),
    (2, "Others"),
    (2, "MV1")).toDF("id", "priority")

  val priorities = Seq(
    "MV1",
    "MV2",
    "VPV",
    "Others").zipWithIndex.toDF("name", "rank")

  table.join(priorities).orderBy($"id",$"rank").where($"priority" === $"name").groupBy($"id").agg(first("priority")).show
  //  val mins = table.join(priorities).where($"priority" === $"name").groupBy("id").agg(min("rank") as "min")
//  val q = mins.join(priorities).where($"min" === $"rank").select("id", "name").show()

}
