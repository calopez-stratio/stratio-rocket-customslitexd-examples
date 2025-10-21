import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.stratio.sparta.OutputExampleWriter
import com.stratio.sparta.sdk.lite.common.models.{OutputOptions, Overwrite}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.crossdata.XDSession
import org.junit.Assert.assertEquals
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import utils.UtilsTestFunctions

import java.io.File

class OutputExampleWriterTest extends AnyFunSuite
  with BeforeAndAfterAll with Matchers with DataFrameSuiteBase with SharedSparkContext {

  var xdSession: XDSession = _
  var testBaseDir: String = _
  var dataTest: DataFrame = _
  var outputOptions: OutputOptions = _
  var outputExampleWriter: OutputExampleWriter = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    testBaseDir = new File("target/hdfs-test-path").getAbsolutePath

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
  }

  override def afterAll(): Unit = {
    super.afterAll()

    spark.close()
    xdSession.closeAll()

  }

  test("Test OK OutputExampleWriter - with property 'repartition' ") {
    outputOptions = OutputOptions(
      saveMode = Overwrite,
      tableName = Some("testFileOutputStep1"),
      primaryKey = None,
      partitionBy = Seq.empty,
      customProperties = Map.empty
    )
    outputExampleWriter = new OutputExampleWriter(xdSession, Map("path" -> testBaseDir, "repartition" -> "1"))
    noException should be thrownBy {
      outputExampleWriter.save(dataTest, outputOptions)
    }
  }

  test("Test OK OutputExampleWriter - with property 'repartition' and 'partitionBy'") {
    outputOptions = OutputOptions(
      saveMode = Overwrite,
      tableName = Some("testFileOutputStep1_1"),
      primaryKey = None,
      partitionBy = Seq("id"),
      customProperties = Map.empty
    )
    outputExampleWriter = new OutputExampleWriter(xdSession, Map("path" -> testBaseDir, "repartition" -> "1"))
    noException should be thrownBy {
      outputExampleWriter.save(dataTest, outputOptions)
    }
  }

  test("Test OK OutputExampleWriter - with property 'coalesce' ") {
    outputOptions = OutputOptions(
      saveMode = Overwrite,
      tableName = Some("testFileOutputStep2"),
      primaryKey = None,
      partitionBy = Seq.empty,
      customProperties = Map.empty
    )
    outputExampleWriter = new OutputExampleWriter(xdSession, Map("path" -> testBaseDir, "coalesce" -> "1"))
    noException should be thrownBy {
      outputExampleWriter.save(dataTest, outputOptions)
    }
  }

  test("Test OK OutputExampleWriter - with property 'coalesce' and 'partitionBy'") {
    outputOptions = OutputOptions(
      saveMode = Overwrite,
      tableName = Some("testFileOutputStep2_2"),
      primaryKey = None,
      partitionBy = Seq("id"),
      customProperties = Map.empty
    )
    outputExampleWriter = new OutputExampleWriter(xdSession, Map("path" -> testBaseDir, "coalesce" -> "1"))
    noException should be thrownBy {
      outputExampleWriter.save(dataTest, outputOptions)
    }
  }

  test("Test OK OutputExampleWriter - with out required property 'path' ") {
    outputOptions = OutputOptions(
      saveMode = Overwrite,
      tableName = Some("testFileOutputStep"),
      primaryKey = None,
      partitionBy = Seq.empty,
      customProperties = Map.empty
    )
    outputExampleWriter = new OutputExampleWriter(xdSession, Map.empty)

    val exception = intercept[NoSuchElementException] {
      outputExampleWriter.save(dataTest, outputOptions)
    }

    assertEquals("Required property 'path' not found", exception.getMessage)
  }

}
