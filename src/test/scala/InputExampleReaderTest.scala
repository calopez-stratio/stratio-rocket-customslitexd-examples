import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.stratio.sparta.InputExampleReader
import com.stratio.sparta.sdk.lite.hybrid.models.ResultHybridData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.crossdata.XDSession
import org.junit.Assert.assertEquals
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import utils.UtilsTestFunctions

class InputExampleReaderTest extends AnyFunSuite
  with BeforeAndAfterAll with Matchers with DataFrameSuiteBase with SharedSparkContext {

  var inputExampleReader: InputExampleReader = _
  var xdSession: XDSession = _
  var dataTest: DataFrame = _
  var genericProperties: Map[String, String] = _
  var resultHybridDataExpected: ResultHybridData = _

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
    resultHybridDataExpected = ResultHybridData(dataTest)
  }

  override def afterAll(): Unit = {
    super.afterAll()

    spark.close()
    xdSession.closeAll()
  }

  test("Test OK InputExampleReader - with valid property query ") {
    dataTest.createTempView("dataViewTest")
    genericProperties = Map(
      "query" -> "select * from dataViewTest"
    )
    inputExampleReader = new InputExampleReader(xdSession, genericProperties)

    val resultHybridDataActual = inputExampleReader.init()
    assertEquals(resultHybridDataExpected.data.toString(), resultHybridDataActual.data.toString())
  }

  test("Test KO InputExampleReader - with out require property query ") {
    genericProperties = Map(
      "testProp" -> "testValue"
    )
    inputExampleReader = new InputExampleReader(xdSession, genericProperties)
    val exception = intercept[NoSuchElementException] {
      inputExampleReader.init()
    }
    assertEquals("Required property 'query' not found", exception.getMessage)
  }

}
