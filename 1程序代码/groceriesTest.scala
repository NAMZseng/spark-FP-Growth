/**
* Scala version  :2.11.12
* Coding         :utf-8
* Author         :Nam Zeng
* CreateTime     :2019/1/13
* Describe       :基于spark的FPGrowth算法实践
* */

import java.io._

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

object groceriesTest {

  /*
  * args[0] :data path
  * args[1] :minSupport
  * args[2] :minConfidence
  * args[3] :numPartitions
  * */
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("FPGrowthTest")
    val sc = new SparkContext(conf)

    val data_path = args(0)
    val minSupport = args(1).toDouble
    val minConfidence = args(2).toDouble
    val numPartitions = args(3).toInt

    val data = sc.textFile(data_path)
    //对CSV文件进行逗号分割
    val transactions = data.map(x => x.split(","))
    transactions.cache()

    val fpg = new FPGrowth()
    fpg.setMinSupport(minSupport)
    fpg.setNumPartitions(numPartitions)

    val model = fpg.run(transactions)

    val writer = new PrintWriter(new File("/root/result.txt"))

    writer.println(
      """----------------------------------------------\n
        |FreqItemsets:
        |\n----------------------------------------------""".stripMargin)
    //查看所有的频繁项集，并且列出它出现的次数
    model.freqItemsets.collect().foreach(itemset => {
      writer.println(itemset.items.mkString("[", ",", "]") + " ：" + itemset.freq)
    })

    writer.println(
      """\n----------------------------------------------\n
        |AssociationRules:
        |\n----------------------------------------------""".stripMargin)
    //通过置信度筛选出推荐规则则
    model.generateAssociationRules(minConfidence).collect().foreach(rule => {
      writer.println(rule.antecedent.mkString("[", ",", "]") + "-->" +
        rule.consequent.mkString("[", ",", "]") + " ：" + rule.confidence)
    })
    writer.close()
  }
}
