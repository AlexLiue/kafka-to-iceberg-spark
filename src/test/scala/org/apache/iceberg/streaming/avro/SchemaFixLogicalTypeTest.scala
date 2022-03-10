package org.apache.iceberg.streaming.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

class SchemaFixLogicalTypeTest extends org.scalatest.FunSuite {

  test("testFixLogicalType-2") {
    val schemaStr = """{"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"C4","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"}"""

    val schema = new Schema.Parser().parse(schemaStr)

    System.out.println(schema.toString(true))

    val schema2 = SchemaFixLogicalType.fixLogicalType(schema)
    System.out.println(schema.toString(true))

   val  after = schema2.getField("after")
    val UPDATE_TIME = after.schema().getTypes.get(1).getField("UPDATE_TIME").schema().getLogicalType


    System.out.println("SSS")
  }

//  test("testFixLogicalType-2") {
//
//  }

}
