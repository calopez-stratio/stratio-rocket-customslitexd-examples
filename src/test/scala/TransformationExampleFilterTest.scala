import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.stratio.sparta.TransformationExampleFilter
import com.stratio.sparta.sdk.lite.hybrid.models.{OutputHybridTransformData, ResultHybridData}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.crossdata.XDSession
import org.junit.Assert.assertEquals
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import utils.UtilsTestFunctions

class TransformationExampleFilterTest  extends AnyFunSuite
  with BeforeAndAfterAll with Matchers with DataFrameSuiteBase with SharedSparkContext {

  var xdSession: XDSession = _
  var dataTest: DataFrame = _
  var resultHybridData: ResultHybridData = _
  var genericProperties: Map[String, String] = _
  var inputData: Map[String, ResultHybridData] = _
  var transformationExampleFilter:TransformationExampleFilter = _
  var outputHybridTransformData:OutputHybridTransformData = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val sparkConf = spark.sparkContext.getConf
      .set("fs.file.impl", classOf[org.apache.hadoop.fs.RawLocalFileSystem].getName)
      .set("fs.hdfs.impl", classOf[org.apache.hadoop.fs.RawLocalFileSystem].getName)

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.RawLocalFileSystem].getName)
    hadoopConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.fs.RawLocalFileSystem].getName)

    val xdBuilder = XDSession.builder()
    sparkConf.getAll.foreach { case (key, value) =>
      xdBuilder.config(key, value)
    }
    xdSession = xdBuilder.create("test-user")

    dataTest = UtilsTestFunctions.createDataTest(xdSession.asSparkSession)
    resultHybridData = ResultHybridData(dataTest)
    inputData = Map("inputData" -> resultHybridData)
  }

  override  def afterAll(): Unit = {
    super.afterAll()

    spark.close()
    xdSession.closeAll()
  }

  test("Test OK TransformationExampleFilter - with valid property 'condition'") {

    import spark.implicits._
    val dataExpected = Seq(
      (1, "Product A", 100.0),
    ).toDF("id", "name", "price")

    genericProperties = Map("condition" -> "name == 'Product A'")
    transformationExampleFilter = new TransformationExampleFilter(xdSession, genericProperties)
    outputHybridTransformData = transformationExampleFilter.transform(inputData)

    assertEquals(dataExpected.toString(),outputHybridTransformData.data.toString())
  }

  test("Test OK TransformationExampleFilter - with out property 'condition'") {

    transformationExampleFilter = new TransformationExampleFilter(xdSession, Map.empty)
    outputHybridTransformData = transformationExampleFilter.transform(inputData)

    assertEquals(dataTest.toString(),outputHybridTransformData.data.toString())
  }

  test("Test OK TransformationExampleFilter - with property 'condition' with space as value") {

    genericProperties = Map("condition" -> " ")
    transformationExampleFilter = new TransformationExampleFilter(xdSession, genericProperties)
    outputHybridTransformData = transformationExampleFilter.transform(inputData)

    assertEquals(dataTest.toString(),outputHybridTransformData.data.toString())
  }

}