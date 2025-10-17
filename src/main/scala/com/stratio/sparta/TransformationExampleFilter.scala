package com.stratio.sparta

import com.stratio.functions.TransformationFunctions
import com.stratio.sparta.sdk.lite.hybrid.models.{OutputHybridTransformData, ResultHybridData}
import com.stratio.sparta.sdk.lite.xd.hybrid.LiteCustomXDHybridTransform
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.crossdata.XDSession

class TransformationExampleFilter(
                                   xdSession: XDSession,
                                   properties: Map[String, String]
                                 ) extends LiteCustomXDHybridTransform(xdSession, properties) {

  override def transform(inputData: Map[String, ResultHybridData]): OutputHybridTransformData = {
    val transformationFunctions = new TransformationFunctions
    val inputDataframe: DataFrame = inputData.head._2.data
    val condition: String = properties.getOrElse("condition", "")

    OutputHybridTransformData(transformationFunctions.filter(inputDataframe, condition))
  }

}
