package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object rides {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataTransformations")
      .master("local[*]")
      .getOrCreate()

    // File Path
    val filePath = "C:/Users/chigb/Downloads/railway.csv"
    //val df = spark.read
      //.option("header", "true")
      //.option("inferSchema", "true")
      //.csv("file:///C:/Users/chigb/Downloads/railway.csv")
    //df.show(5)

    // 1. Read the CSV File
    val df = spark.read
      .option("header", "true") // Treat the first row as headers
      .option("inferSchema", "true") // Infer data types automatically
      .csv(filePath)

    println("Original DataFrame:")
    df.show(5)
    df.printSchema() // Print schema to verify column names and types

    // 2. Filter data where Payment Method == "Credit Card"
    val filteredDF = df.filter(col("Payment Method") === "Credit Card")
    println("Filtered DataFrame (Payment Method == 'Credit Card'):")
    filteredDF.show(5)

    // 3. Select specific columns
    val selectedColumnsDF = df.select("Transaction ID", "Time of Purchase", "Purchase Type", "Price")
    println("Selected Columns DataFrame:")
    selectedColumnsDF.show(5)

    // 4. Add Peak_Price column (Price + 15)
    val modifiedDF = df.withColumn("Peak_Price", col("Price") + 15)
    println("Modified DataFrame with Peak_Price:")
    modifiedDF.show(5)

    // 5. Drop the Railcard column
    val finalDF = modifiedDF.drop("Railcard")
    println("Final DataFrame (without Railcard):")
    finalDF.show(5)

    // Stop SparkSession
    spark.stop()
  }
}
