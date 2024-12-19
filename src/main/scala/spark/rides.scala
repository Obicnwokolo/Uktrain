package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, DataFrame}

object rides {
  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("PostgreSQLExample")
      .master("local[*]")
      .getOrCreate()

    // Define JDBC connection parameters
    val jdbcUrl = "jdbc:postgresql://18.132.73.146:5432/testdb"
    val dbProperties = new java.util.Properties()
    dbProperties.setProperty("user", "consultants")  // Your database username
    dbProperties.setProperty("password", "WelcomeItc@2022")  // Your database password
    dbProperties.setProperty("driver", "org.postgresql.Driver")

    // Read data from the PostgreSQL table into a DataFrame
    val df = spark.read
      .jdbc(jdbcUrl, "railway", dbProperties)  // Replace "your_table_name" with your table name

    // Show the first 5 rows of the DataFrame
    df.show(5)

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

    // Write DataFrame to Hive table
    df.write
      .mode("overwrite")  // Use append for adding data without overwriting
      .saveAsTable("bigdata_nov_2024.railway")  // Specify your database and table name

    // Stop SparkSession
    spark.stop()
  }
}
