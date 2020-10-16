package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyKafkaUtil, PropertiesUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object GmvApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.读取Kafka数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(ssc, GmallConstant.KAFKA_TOPIC_ORDER_INFO)

    //4.将每一行数据转换为样例类对象,并做数据脱敏
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
      //a.将数据转换为样例类对象
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      //b.创建日期及小时赋值  create_time  =>>>  2020-10-16 09:45:50
      val dateTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = dateTimeArr(0)
      orderInfo.create_hour = dateTimeArr(1).split(":")(0)
      //c.脱敏数据:手机号
      val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"
      //d.返回结果
      orderInfo
    })

    //5.将数据保存至Phoenix
    orderInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(PropertiesUtil.load("config.properties").getProperty("phoenix.gmv.table"),
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create(),
        Some(PropertiesUtil.load("config.properties").getProperty("phoenix.zk.url")))
    })

    orderInfoDStream.cache()
    orderInfoDStream.print()

    //6.开启任务
    ssc.start()
    ssc.awaitTermination()

  }


}
