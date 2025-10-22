package com.stratio.sparta

import com.stratio.sparta.sdk.lite.common.SpartaUDF
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class ToUpperCaseUDF extends SpartaUDF {

  val name = "uppercaseSparta"

  private val upper: String => String = _.toUpperCase

  val userDefinedFunction: UserDefinedFunction = udf(upper)

}

class ConcatUDF extends SpartaUDF {

  val name = "concatSparta"

  private val concat: (String, String) => String = (str1, str2) => s"$str1/$str2"

  val userDefinedFunction: UserDefinedFunction = udf(concat)
}
