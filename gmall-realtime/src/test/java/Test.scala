import com.atguigu.utils.RedisUtil
import redis.clients.jedis.Jedis

object Test {

  def main(args: Array[String]): Unit = {

    val client: Jedis = RedisUtil.getJedisClient

    client.set("中文", "文浩")

    client.close()
  }

}
