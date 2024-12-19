package spark
//import spark.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object rides {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataTransformations")
      .master("local[*]")
      .getOrCreate()

    // 1. Read the CSV File
    val filePath = "C:/Users/chigb/Downloads/railway.csv"
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    println("Original DataFrame:")
    df.show(5)

    // Filtered the data based on Purchase Type(online)
    val filtered_df = df.filter(col("Payment Method") === "Credit Card")
    println("filtered DataFrame (Purchase Type == Online):")
    filtered_df.show(5)

    // Select specific Columns
    val selectCol_df = df.select("Transaction ID", "Time of Purchase", "Purchase Type", "Price")
    selectCol_df.show(5)

    // Introduce Peak-price which is price (price + 15)
    val Mod_df= df.withColumn("Peak_Price", col("Price" )+ 15)
    Mod_df.show(5)

    // Drop a column
    val df_mod =Mod_df.drop("Railcard")
    df_mod.show(5)

  }
}