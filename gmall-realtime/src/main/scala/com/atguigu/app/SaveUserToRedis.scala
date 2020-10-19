package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object SaveUserToRedis {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.读取Kafka用户主题创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(ssc, GmallConstant.KAFKA_TOPIC_USER_INFO)

    //4.提取Value
    val userInfoStrDStream: DStream[String] = kafkaDStream.map(_.value())

    //5.将数据写入Redis
    userInfoStrDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //b.遍历数据
        iter.foreach(userInfoStr => {
          //转换为样例类对象
          val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
          //String
          val redieKey = s"UserInfo:${userInfo.id}"
          jedisClient.set(redieKey, userInfoStr)
        })

        //c.释放连接
        jedisClient.close()

      })

    })

    //6.开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
