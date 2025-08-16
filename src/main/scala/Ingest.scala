import org.apache.spark.sql.SparkSession
import java.sql.DriverManager

object Ingest{

  def createDb(jdbcUrl: String, username: String, password: String, dbName:String) {
    import java.sql.DriverManager

    val connection = DriverManager.getConnection(jdbcUrl,username,password)
    val statement = connection.createStatement()
    statement.executeUpdate(s"CREATE DATABASE $dbName;")
    statement.close()
    connection.close()
  }

  def main(args: Array[String]): Unit = {

    println("Hello World")
    val spark = SparkSession
      .builder()
      .appName("POC")
      //    .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val jdbc_url = s"jdbc:postgresql://localhost:5432/postgres"
    val username = args(0)
    val password = args(1)
    val dbName = args(2)

    try {
      val dbConn = DriverManager.getConnection(jdbc_url, username, password)

      val properties = new java.util.Properties() {
        {
          put("user", username)
          put("password", password)
          put("driver", "org.postgresql.Driver")
        }
      }

      import spark.implicits._

      val df1 = spark.read
        .option("header", value = true)
        .csv("C:\\data\\POC\\CodeYodha_1.csv")

      val after = df1.dropDuplicates(Seq("E-mail", "Contact Number"))

      after.write.mode("overwrite").jdbc(jdbc_url, "POC", properties)
      dbConn.close()
      println("Write Successful")



    }
    catch {
      case e: java.sql.SQLException if e.getSQLState == "3D000" =>
        // 3D000 = invalid_catalog_name (database does not exist in PostgreSQL)
        println(s"Database not found: ${e.getMessage}")
        createDb(jdbc_url, username, password, dbName)
        println("The Database has been created")
        Ingest.main(args)
      case e: Exception =>
        if (spark.catalog.databaseExists(s"$dbName")== "true") println("The database already exists. No need to create again")
        else println(s"Other connection error: ${e.getMessage}")



    }
  }
}