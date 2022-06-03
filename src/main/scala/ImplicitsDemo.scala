import org.apache.spark.sql.functions.expr

object ImplicitsDemo extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
// first excer
  val dept = Seq(
    ("50000.0#0#0#", "#"),
    ("0@1000.0@", "@"),
    ("1$", "$"),
    ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")

  val solution = dept.withColumn("split_values", expr("""split(values, concat('\\', delimiter))"""))
  solution.show



// second excer
val input = Seq(
  Seq("a","b","c"),
  Seq("X","Y","Z")).toDF

  val array_size = input.as[Seq[String]].head.size


  (0 until array_size).foldLeft(input) { (result, n) => result.withColumn(s"$n", $"value"(n)) }.drop("value").show


  input.select(
    (0 until array_size).map(i => $"value".getItem(i).as(s"$i")): _*
  ).show


// third excer

  val nums = Seq(Seq(1, 2, 3)).toDF("nums")

  val num = nums.flatMap { r =>
    val ns = r.getSeq[Int](0)
    ns.map(n => (ns, n))
  }

  num.toDF("nums", "num").show()



}

