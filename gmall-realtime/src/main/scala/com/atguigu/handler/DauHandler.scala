package com.atguigu.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {

  private val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  /**
    * 同批次去重(根据mid做去重)
    *
    * @param filterByRedisLogDStream 根据Redis去重之后的数据
    * @return
    */
  def filterByMid(filterByRedisLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    //1.将数据转换为元组 ==> (mid,StartUpLog)
    val midDateToLogDStream: DStream[(String, StartUpLog)] = filterByRedisLogDStream.map(startUpLog => {
      (s"${startUpLog.mid}-${startUpLog.logDate}", startUpLog)
    })

    //2.按照mid分组
    val midDateToLogDStreamIter: DStream[(String, Iterable[StartUpLog])] = midDateToLogDStream.groupByKey()

    //3.组内数据排序并取第一条
    //a.看是否需要Key
    //b.是否需要压平操作
    //需要Key且需要压平     ==> flatMapValues
    //需要Key但不需要压平   ==> mapValues
    //不需要Key但需要压平   ==> flatMap
    //不需要Key也不需要压平 ==> map
    midDateToLogDStreamIter.flatMap { case (_, iter) =>
      iter.toList.sortWith(_.ts < _.ts).take(1)
    }

  }

  /**
    * 跨批次去重(根据Redis中保存的数据,做过滤)
    *
    * @param startUpLogDStream 从Kafka消费到的原始数据
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {

    //方案一:每条数据都获取跟释放连接,效率低
    val value1: DStream[StartUpLog] = startUpLogDStream.filter(startUpLog => {
      //a.获取Redis客户端
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.查询Redis中是否存在该Mid
      val exist: lang.Boolean = jedisClient.sismember(s"DAU:${startUpLog.logDate}", startUpLog.mid)
      //c.释放连接
      jedisClient.close()
      //d.返回数据
      !exist
    })

    //方案二:采用一个分区获取一次连接的方式
    val value2: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      rdd.mapPartitions(iter => {
        //a.获取Redis客户端
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //b.遍历数据进行过滤
        val logs: Iterator[StartUpLog] = iter.filter(startUpLog => {
          !jedisClient.sismember(s"DAU:${startUpLog.logDate}", startUpLog.mid)
        })
        //c.释放连接
        jedisClient.close()
        //返回数据
        logs
      })
    })

    //方案三:使用广播变量将前置批次Redis中所有数据进行广播
    val value3: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {

      //a.获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.查询Redis今天所有登录过的mid
      val midSet: util.Set[String] = jedisClient.smembers(s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}")
      //c.广播
      val midSetBC: Broadcast[util.Set[String]] = sc.broadcast(midSet)
      //d.释放连接
      jedisClient.close()

      //e.在Executor进行过滤操作
      rdd.filter(startUpLog => !midSetBC.value.contains(startUpLog.mid))
    })

    //    value1
    //    value2
    value3
  }


  /**
    * 将去重之后的数据中的Mid写入Redis(给当天以后的批次去重使用)
    *
    * @param startUpLogDStream 经过两次去重之后的数据集
    */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]): Unit = {

    startUpLogDStream.foreachRDD(rdd => {

      //使用分区操作代替单条数据操作,减少连接的创建与释放
      rdd.foreachPartition(iter => {

        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //b.遍历数据集,操作数据
        iter.foreach(startUpLog => {
          val redisKey = s"DAU:${startUpLog.logDate}"
          jedisClient.sadd(redisKey, startUpLog.mid)
        })

        //c.释放连接
        jedisClient.close()

      })

    })

  }


}
