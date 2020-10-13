package com.atguigu.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MyKafkaUtil {

  //1.创建配置信息对象
  private val properties: Properties = PropertiesUtil.load("config.properties")

  //2.用于初始化链接到集群的地址
  val broker_list: String = properties.getProperty("kafka.broker.list")
  private val groupId: String = properties.getProperty("kafka.group.id")

  //3.kafka消费者配置
  val kafkaParam = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //消费者组
    "group.id" -> groupId,
    //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //可以使用这个配置，latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  // 创建DStream，返回接收到的输入数据
  // LocationStrategies：根据给定的主题和集群地址创建consumer
  // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
  // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
  // ConsumerStrategies.Subscribe：订阅一系列主题
  def getKafkaDStream(ssc: StreamingContext, topic: String): InputDStream[ConsumerRecord[String, String]] = {

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaParam))

    kafkaDStream

  }

}
