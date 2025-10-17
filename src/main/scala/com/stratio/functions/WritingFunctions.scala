package com.stratio.functions

import org.apache.spark.sql.{DataFrame, SaveMode}

class WritingFunctions {

  def writeDataToParquet(
                          data: DataFrame,
                          partitionBy: Seq[String] = Seq.empty,
                          repartition: Int = 0,
                          coalesce: Int = 0,
                          path: String,
                          mode: SaveMode = SaveMode.ErrorIfExists
                        ): Unit = {

    var dataFrame = data

    if (repartition >= 1) {
      dataFrame = data.repartition(repartition)
    } else if (coalesce >= 1) {
      dataFrame = data.coalesce(coalesce)
    }

    val writer = dataFrame.write.mode(mode)
    val validPartitionColumns = partitionBy.filter(_.trim.nonEmpty)

    if (validPartitionColumns.nonEmpty) {
      writer.partitionBy(validPartitionColumns: _*).parquet(path)
    } else {
      writer.parquet(path)
    }
  }
}
