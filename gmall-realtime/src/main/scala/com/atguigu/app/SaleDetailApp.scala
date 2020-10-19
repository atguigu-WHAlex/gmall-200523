package com.atguigu.app

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.util.JDBCUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis

import collection.JavaConverters._
import org.json4s.native.Serialization
import redis.clients.util.RedisInputStream

import scala.collection.mutable.ListBuffer

object SaleDetailApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.读取Kafka中orderInfo和OrderDetail数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(ssc, GmallConstant.KAFKA_TOPIC_ORDER_INFO)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(ssc, GmallConstant.KAFKA_TOPIC_ORDER_DETAIL)

    //4.将两个流转换为样例类对象,并转换为K_V结构
    val orderIdToInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(record => {
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
      (orderInfo.id, orderInfo)
    })

    val orderIdToDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(record => {
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      (orderDetail.order_id, orderDetail)
    })

    //普通JOIN(不对跨批次数据做处理)
    //    val joinDStream: DStream[(String, (OrderInfo, OrderDetail))] = orderIdToInfoDStream.join(orderIdToDetailDStream)
    //    joinDStream.print()

    //5.加缓存的JOIN方式(fullJoin)
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)

    val noUserSaleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(iter => {

      //创建集合用于存放JOIN上的数据集
      // 当前批次
      // Info数据和detail的前置批次
      // detail数据和Info数据的前置批次
      val details = new ListBuffer[SaleDetail]
      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

      //遍历迭代器
      iter.foreach { case (orderId, (infoOpt, detailOpt)) =>

        val orderInfoKey = s"OrderInfo:$orderId"
        val orderDetailKey = s"OrderDetail:$orderId"

        //1.判断infoOpt不为空
        if (infoOpt.isDefined) {

          //获取info数据
          val orderInfo: OrderInfo = infoOpt.get

          //1.1 判断detailOpt是否为空,如果不为空,则JOIN上,写入details
          if (detailOpt.isDefined) {
            val orderDetail: OrderDetail = detailOpt.get
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            details += saleDetail
          }

          //1.2 将info数据转换为JSON字符串写入Redis
          //val str: String = JSON.toJSONString(orderInfo) //编译过不了
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedisClient.set(orderInfoKey, orderInfoJson)
          jedisClient.expire(orderInfoKey, 100)

          //1.3 查询Detail前置批次,如果有数据,则JOIN上,写入details
          val orderDetailSet: util.Set[String] = jedisClient.smembers(orderDetailKey)
          for (orderDetailJson <- orderDetailSet.asScala) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            details += new SaleDetail(orderInfo, orderDetail)
          }

        } else {
          //2.判断infoOpt为空

          //获取Detail数据
          val orderDetail: OrderDetail = detailOpt.get

          //判断info前置批次是否有数据
          if (jedisClient.exists(orderInfoKey)) {
            //2.1 如果有数据,则JOIN上,写入details
            val orderInfoJson: String = jedisClient.get(orderInfoKey)
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            details += new SaleDetail(orderInfo, orderDetail)

          } else {
            //2.2 如果没有数据,则将Detail数据转换为JSON字符串写入Redis
            val orderDetailJson: String = Serialization.write(orderDetail)
            jedisClient.sadd(orderDetailKey, orderDetailJson)

          }
        }
      }

      //释放Redis连接
      jedisClient.close()
      //最终返回值
      details.toIterator
    })

    //打印测试
    //    noUserSaleDetailDStream.print()
    //6.根据userId获取Redis中用户信息,补全信息
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(iter => {

      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient


      //遍历数据,根据userId获取Redis中用户信息
      val details: Iterator[SaleDetail] = iter.map(noUserSaleDetail => {

        val userRedisKey = s"UserInfo:${noUserSaleDetail.user_id}"

        if (jedisClient.exists(userRedisKey)) {
          val userInfoStr: String = jedisClient.get(userRedisKey)
          val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
          noUserSaleDetail.mergeUserInfo(userInfo)

        } else {
          //MySQL
          val connection: Connection = JdbcUtil.getConnection
          val userInfoJson: String = {
            JdbcUtil.getUserInfoFromMysql(connection,
              "select * from gmall200523.user_info where id =?",
              Array(noUserSaleDetail.user_id))
          }
          val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
          noUserSaleDetail.mergeUserInfo(userInfo)
          connection.close()
        }

        noUserSaleDetail
      })

      //释放连接
      jedisClient.close()


      //返回数据
      details

    })

    //7.写入ES
    val format = new SimpleDateFormat("yyyy-MM-dd")
    saleDetailDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {
        //创建索引名 gmall0523_sale_detail-2020-10-19
        val indexName = s"${PropertiesUtil.load("config.properties").getProperty("es.sale.prefix")}-${format.format(new Date(System.currentTimeMillis()))}"
        val docList: List[(String, SaleDetail)] = iter.toList.map(saleDetail => (s"${saleDetail.order_id}-${saleDetail.order_detail_id}", saleDetail))
        MyEsUtil.insertBulk(indexName,docList)
      })

    })

    //8.启动任务
    ssc.start()
    ssc.awaitTermination()

  }


}
