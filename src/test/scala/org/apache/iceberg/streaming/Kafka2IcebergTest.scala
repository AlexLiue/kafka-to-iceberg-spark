package org.apache.iceberg.streaming

import org.junit.Test

class Kafka2IcebergTest extends org.scalatest.FunSuite {

  /* Test hadoop catalog */
  @Test
  def test():Unit= {
    val argStr = "runEnv=test master=local[3] confKey=mysql:HADOOP%G_GB18030 batchDuration=15  "
    Kafka2Iceberg.main(argStr.split(" "))
  }


}
