package com.stratio.sparta

import com.stratio.functions.{ValidationFunctions, WritingFunctions}
import com.stratio.sparta.sdk.lite.common.models.OutputOptions
import com.stratio.sparta.sdk.lite.xd.common.LiteCustomXDOutput
import com.stratio.sparta.sdk.lite.common.models.{SaveMode => SaveModeSDK}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.crossdata.XDSession
import org.apache.spark.sql.SaveMode

//noinspection ScalaDeprecation
class OutputExampleWriter(
                           xdSession: XDSession,
                           properties: Map[String, String]
                         )
  extends LiteCustomXDOutput(xdSession, properties) {

  private val writingFunctions = new WritingFunctions
  private val validationFunctions = new ValidationFunctions

  override def save(data: DataFrame, outputOptions: OutputOptions): Unit = {

    val saveModeSDK: SaveModeSDK = outputOptions.saveMode
    val partitionBy: Seq[String] = outputOptions.partitionBy
    val tableName: String = outputOptions.tableName.get
    val path: String = validationFunctions.getRequiredProperty("path", properties)
    val repartition: Int = properties.getOrElse("repartition", "0").toInt
    val coalesce: Int = properties.getOrElse("coalesce", "0").toInt

    writingFunctions.writeDataToParquet(
      data,
      partitionBy,
      repartition,
      coalesce,
      s"$path/$tableName",
      SaveMode.valueOf(saveModeSDK.toString)
    )
  }

  override def save(data: DataFrame, saveMode: String, saveOptions: Map[String, String]): Unit = ()
}
