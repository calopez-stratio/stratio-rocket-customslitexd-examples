import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.stratio.sparta.{ConcatUDF, ToUpperCaseUDF}
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import utils.UtilsTestFunctions
import org.apache.spark.sql.functions.col

class TransformationExampleUdfsTest extends AnyFunSuite
  with BeforeAndAfterAll with Matchers with DataFrameSuiteBase with SharedSparkContext {

  var dataTest: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    dataTest = UtilsTestFunctions.createDataTest(spark)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.close()
  }

  test("Test OK ToUpperCaseUDF - Converting to uppercase") {
    val udfInstance = ToUpperCaseUDF()

    val result = dataTest.withColumn("uppercased", udfInstance.userDefinedFunction(col("name")))
      .collect()

    result should have length 3

    result(0).getString(3) shouldBe "PRODUCT A"
    result(1).getString(3) shouldBe "PRODUCT B"
    result(2).getString(3) shouldBe "PRODUCT C"
  }

  test("Test OK ConcatUDF - Concatened columns") {
    val udfInstance = ConcatUDF()

    val result = dataTest.withColumn(
      "columnConcatened",
      udfInstance.userDefinedFunction(
        col("id").cast("string"),
        col("name")
      )
    ).collect()

    result should have length 3

    result(0).getString(3) shouldBe "1/Product A"
    result(1).getString(3) shouldBe "2/Product B"
    result(2).getString(3) shouldBe "3/Product C"
  }

}
