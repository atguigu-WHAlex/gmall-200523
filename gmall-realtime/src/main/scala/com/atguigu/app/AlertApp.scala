package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.读取Kafka主题创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(ssc, GmallConstant.KAFKA_TOPIC_EVENT)

    //4.将每行数据转换为样例类对象
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val eventLogDStream: DStream[EventLog] = kafkaDStream.map(record => {

      //转换为样例类对象
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

      //处理日期&小时
      val dateHourStr: String = sdf.format(new Date(eventLog.ts))
      val dateHourArr: Array[String] = dateHourStr.split(" ")
      eventLog.logDate = dateHourArr(0)
      eventLog.logHour = dateHourArr(1)

      //返回数据
      eventLog
    })

    //5.按照mid分组
    val midToLogIter: DStream[(String, Iterable[EventLog])] = eventLogDStream.map(eventLog => (eventLog.mid, eventLog))
      .groupByKey()

    //6.开窗,5min的窗口
    val windowDStream: DStream[(String, Iterable[EventLog])] = midToLogIter.window(Minutes(5))

    //7.窗口内部做数据分析
    //    三次及以上用不同账号:登陆过的uid个数 >= 3
    //    没有浏览商品:反向考虑,只要有浏览行为,则不产生预警日志
    val boolToAlertInfo: DStream[(Boolean, CouponAlertInfo)] = windowDStream.map { case (mid, iter) =>

      //创建HashSet用于存放uid
      val uids = new util.HashSet[String]()
      //创建HashSet用于存放优惠券涉及的商品ID
      val itemIds = new util.HashSet[String]()
      //创建集合用于存放发生过的行为
      val events = new util.ArrayList[String]()

      //定义标签用于记录是否存在浏览商品行为
      var noClick: Boolean = true

      //遍历iter
      breakable {
        iter.foreach(eventLog => {
          //添加事件信息
          events.add(eventLog.evid)
          //统计领券的uid
          if ("coupon".equals(eventLog.evid)) {
            uids.add(eventLog.uid)
            itemIds.add(eventLog.itemid)
          } else if ("clickItem".equals(eventLog.evid)) {
            noClick = false
            break()
          }
        })
      }

      //产生疑似预警日志
      (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))

    }

    //8.产生预警日志
    val alertInfoDStream: DStream[CouponAlertInfo] = boolToAlertInfo.filter(_._1).map(_._2)

    alertInfoDStream.cache()
    alertInfoDStream.count().print()

    //9.写入ES
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    alertInfoDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //创建索引名 gmall_coupon_alert-2020-10-16
        val indexName = s"${PropertiesUtil.load("config.properties").getProperty("es.alert.prefix")}-${format.format(new Date(System.currentTimeMillis()))}"

        //准备数据,添加docId
        val hourMinu: String = format2.format(new Date(System.currentTimeMillis()))
        val docList: List[(String, CouponAlertInfo)] = iter.toList.map(alertInfo => (s"${alertInfo.mid}-$hourMinu", alertInfo))

        //将数据写入ES
        MyEsUtil.insertBulk(indexName, docList)

      })
    })

    //10.开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
