package com.dounine.compute.util

import java.util
import java.util.Properties

import org.apache.commons.lang3.StringUtils
import redis.clients.jedis.{JedisPool, JedisPoolConfig, Protocol}

class RedisObj {
  def jedisPool: JedisPool = init()
  private def init(): JedisPool = {
    val config = new JedisPoolConfig
    val redisProperties: Properties = PropsUtils.properties("db")
    val redisPassword: String = redisProperties.getProperty("redis.password")
    var jedisPool = new JedisPool(config, redisProperties.getProperty("redis.host"), Integer.parseInt(redisProperties.getProperty("redis.port")), 0)
    if (StringUtils.isNotBlank(redisPassword)) {
      jedisPool = new JedisPool(config, redisProperties.getProperty("redis.host"), Integer.parseInt(redisProperties.getProperty("redis.port")), 0, redisPassword, Protocol.DEFAULT_DATABASE)
    }
    jedisPool
  }
}

object RedisUtil {
  private val pools = new util.HashMap[String, JedisPool]()
  def apply(poolName: String = ""): JedisPool = this.synchronized {
    if (pools.isEmpty || !pools.containsKey(poolName)) {
      this.synchronized {
        pools.put(poolName,new RedisObj().jedisPool)
      }
    }
    pools.values().iterator().next()
  }
}
