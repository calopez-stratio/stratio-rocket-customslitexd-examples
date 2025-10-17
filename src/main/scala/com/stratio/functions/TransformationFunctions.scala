package com.stratio.functions

import org.apache.spark.sql.DataFrame

class TransformationFunctions {

  def filter(df: DataFrame, condicion: String): DataFrame = {
    if (condicion == null || condicion.trim.isEmpty) {
      df
    } else {
      df.filter(condicion.trim)
    }
  }
}
