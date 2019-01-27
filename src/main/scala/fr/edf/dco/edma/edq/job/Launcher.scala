package fr.edf.dco.edma.edq.job

import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import fr.edf.dco.edma.configuration.configurationLoader
import fr.edf.dco.edma.configuration.EdmaDataQualityConfiguration
import fr.edf.dco.edma.edq.input.{ConnexionHelper, HBaseInput}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import fr.edf.dco.edma.edq.dataNode.{DataNode, Validation, checkResult}
import fr.edf.dco.edma.edq.output.HbaseOutput
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes

case class UnvalidateData(checkId:String,unvalidateRowKey:List[String],nbOfRowsUnvalidated:Int)

object Launcher extends ConnexionHelper {

  def main(args: Array[String]): Unit = {
    val configs: Seq[Config] = configurationLoader.parse(".")
    configs.map(conf => launchTest(conf))
  }

  def launchTest(conf: Config): Unit = {
    val edmaDataQuality = new EdmaDataQualityConfiguration(conf)

    val result: RDD[DataNode] = edmaDataQuality.getType() match {
      case "HBase" => HBaseInput.getTable(edmaDataQuality.getTable())
    }

    val unValidateData = collectUnvalidateData {
      result.flatMap(dataNode => Validation.validate(dataNode,edmaDataQuality.getFieldsChecks()))
    }


    HbaseOutput.save(unValidateData,edmaDataQuality.getTable())
  }

  def collectUnvalidateData(datanodes : RDD[checkResult]) = {
    datanodes.filter(!_.isChecked).map(data => (data.id,(List(data.rowKey),1)))
      .reduceByKey((one,two) => (one._1 ++ two._1,one._2 + two._2))
      .map(data => UnvalidateData(data._1,data._2._1,data._2._2))
  }

}
