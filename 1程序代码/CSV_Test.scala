/**
  * Scala version  :2.11.12
  * Coding         :utf-8
  * Author         :Nam Zeng
  * CreateTime     :2019/1/13
  * Describe       :CSV文件读取测试
  * */

import java.io.{File, PrintWriter}

import scala.io.Source

object CSV_Test {
  def main(args: Array[String]): Unit = {
    val filename = "F:\\Linux_CentOS_7\\Hadoop服务集群\\数据集\\PFGrowth\\groceries_demo.txt"
    val bufferedSource = Source.fromFile(filename)
    val writer = new PrintWriter(new File("F:\\Linux_CentOS_7\\Hadoop服务集群\\数据集\\PFGrowth\\out.txt"))

    for (line <- bufferedSource.getLines) {
      val cols = line.split(",")/*.map(_.trim)*/
      for (st <- cols) {
        writer.print(st + "|")
      }
      writer.println()
    }

    bufferedSource.close
    writer.close()
  }
}
