package com.atguigu.sparkCore.chapter05_Mysql_Hbase

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object chapter05_Mysql {

  def main(args: Array[String]): Unit = {

//    mysqlselect()

    mysqlInsertPartition()


  }


  //mysql插入数据优化  foreachpartition
  def mysqlInsertPartition(): Unit ={
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val value: RDD[(Int, String)] = sc.parallelize(Array((3,"aa"),(4,"bb"),(5,"cc")))

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"
    val user = "root"
    val passwd = "root"

    val sql = "insert into info(id,name) values(?,?)"

    value.foreachPartition{ datas =>

      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, user, passwd)

      datas.foreach{
        case(id,name) => {
          val statement: PreparedStatement = connection.prepareStatement(sql)
          statement.setInt(1,id)
          statement.setString(2,name)
          statement.execute()

          //boolean int
          statement.close()

        }
      }

      connection.close()

    }

  }

  //mysql查询数据
  def mysqlselect(): Unit ={

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"
    val user = "root"
    val passwd = "root"

    //connection必须在里面构造
//    Class.forName(driver)
//    val connection: Connection = DriverManager.getConnection(url,user,passwd)

    val sql = "select * from info where id >= ? and id <= ?"

    val value: JdbcRDD[(Int, String)] = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, user, passwd)
    },
      sql,
      0,
      4,
      2,
      rs => (rs.getInt(1), rs.getString(2))
    )
    value

    value.foreach(println)

  }

}
