import SparkDemo.spark

//PS C:\spark\bin> .\spark-submit --class Wed \c:\Users\m.sabatowski\IdeaProjects\Spark-demo\target\scala-2.12\spark-demo_2.12-0.1.0-SNAPSHOT.jar C:\Users
//  \m.sabatowski\IdeaProjects\Spark-demo\iris.csv


import org.apache.spark.sql.{DataFrame, functions}

object Wed extends App {


  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  val path = if (args.length > 0) args(0) else "iris.csv"


  val df: DataFrame = spark.read.options(Map("delimiter" -> ",", "header" -> "true")).csv(path)
  //  df.printSchema()
//  df.show()
  val df1 = df.withColumn("variety", functions.upper(df("variety")))
//  df1.show()


//bround - rounds numbers down, use ` ` on a column name to when it contains . ,
  // use cast() to change a data type of a column,
  // rand function creates new column with random values from 0 to 1


  val df2 = df1.withColumn("petal.length", functions.round(df1("`petal.length`").cast("int")))
  df2.printSchema()
  val df3 = df2.withColumn("randomcol", functions.rand())


  df3.show(truncate = false)
  df3.summary().show()

}
