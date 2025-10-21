package utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object UtilsTestFunctions {

  def createDataTest(
                      sparkSession: SparkSession
                    ): DataFrame = {
    import sparkSession.implicits._
    val data = Seq(
      (1, "Product A", 100.0),
      (2, "Product B", 200.0),
      (3, "Product C", 300.0)
    ).toDF("id", "name", "price")
    data
  }

}
