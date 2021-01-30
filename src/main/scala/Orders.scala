import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Orders extends App {

  val spark = SparkSession.builder()
    .appName("Orders")
    .config("spark.master", "local")
    .getOrCreate()

  val data: DataFrame = spark.read
    .option("multiline","true")
    .json("src/main/resources/orders.json")

  val ordersDF = data
    .withColumn("order", explode(col("OrderMaster.Order.item")))
    .select("order.*")
    .withColumn("item", explode(col("OrderItems.item")))
    .select("*","item.*")
    .drop("OrderItems", "item")
  ordersDF.show()

}
