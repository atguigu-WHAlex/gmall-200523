package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstant
import com.atguigu.handler.DauHandler
import com.atguigu.utils.{MyKafkaUtil, PropertiesUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.消费Kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(ssc, GmallConstant.KAFKA_TOPIC_START)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    //4.将数据转换为样例类对象(为了后续方便处理数据)
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
      //a.取出Value
      val value: String = record.value()
      //b.转换为样例类对象
      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
      //c.取出数据中的时间戳
      val ts: Long = startUpLog.ts
      //d.格式化时间
      val dateHourStr: String = sdf.format(new Date(ts))
      //e.按照空格切分
      val dateHourArr: Array[String] = dateHourStr.split(" ")
      //h.给日期以及时间重新赋值
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)

      startUpLog
    })

    startUpLogDStream.cache()
    startUpLogDStream.count().print()

    //5.跨批次去重(根据Redis中保存的数据,做过滤)
    val filterByRedisLogDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream, ssc.sparkContext)

    filterByRedisLogDStream.cache()
    filterByRedisLogDStream.count().print()

    //6.同批次去重(根据mid做去重)
    val filterByMidLogDStream: DStream[StartUpLog] = DauHandler.filterByMid(filterByRedisLogDStream)

    filterByMidLogDStream.cache()
    filterByMidLogDStream.count().print()

    //7.将去重之后的数据中的Mid写入Redis(给当天以后的批次去重使用)
    DauHandler.saveMidToRedis(filterByMidLogDStream)

    //8.将去重之后的数据明细写入Phoenix
    filterByMidLogDStream.foreachRDD { rdd =>
      rdd.saveToPhoenix(PropertiesUtil.load("config.properties").getProperty("phoenix.dau.table"),
        //        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        classOf[StartUpLog].getDeclaredFields.map(_.getName.toUpperCase()),
        new Configuration,
        Some(PropertiesUtil.load("config.properties").getProperty("phoenix.zk.url")))
    }

    //9.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
