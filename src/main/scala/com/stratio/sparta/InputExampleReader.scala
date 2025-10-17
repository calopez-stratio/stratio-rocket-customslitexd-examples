package com.stratio.sparta

import com.stratio.functions.ValidationFunctions
import com.stratio.sparta.sdk.lite.hybrid.models.ResultHybridData
import com.stratio.sparta.sdk.lite.xd.hybrid.LiteCustomXDHybridInput
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.crossdata.XDSession

class InputExampleReader(
                          xdSession: XDSession,
                          properties: Map[String, String]
                        )
  extends LiteCustomXDHybridInput(xdSession, properties) {

  private val validationFunctions = new ValidationFunctions

  override def init(): ResultHybridData = {

    val query: String = validationFunctions.getRequiredProperty("query", properties)
    val sparkSession: SparkSession = xdSession.asSparkSession
    val dataFrame: DataFrame = sparkSession.sql(query)

    ResultHybridData(dataFrame)
  }
}
