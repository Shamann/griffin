/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.griffin.measure.sink

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import redis.clients.jedis.{Jedis, JedisPool}

import org.apache.griffin.measure.execution.Measure
import org.apache.griffin.measure.step.builder.ConstantColumns
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.griffin.measure.utils.ParamUtil.ParamMap

final case class RedisSink(config: Map[String, Any], jobName: String, timeStamp: Long)
    extends Sink {

  RedisConnection.init(config)

  val block = true
  val Unknown = "UNKNOWN"
  val RecordsPath = "records.path"
  val OutputFile = "output.file"
  val Delimiter = "delimiter"
  var appId: String = null

  override def validate(): Boolean = RedisConnection.dataConf.available

  override def open(applicationId: String): Unit = {
    appId = applicationId
    info(
      s"Opened RedisSink for job with name '$jobName', " +
        s"timestamp '$timeStamp' and applicationId '$applicationId'")
  }

  override def sinkMetrics(metrics: Map[String, Any]): Unit = {
    redisInsert(metrics)
  }

  override def sinkBatchRecords(dataset: DataFrame, key: Option[String]): Unit = {
    val recordsPath = config.getString(RecordsPath, "").trim
    val outputPath = config.getString(OutputFile, defValue = "").trim
    val delimiter = config.getString(Delimiter, defValue = ",")
    var outputFilePath: String = outputPath
    if (!outputPath.endsWith(".csv")) {
      outputFilePath = s"${outputPath}.csv"
    }
    val path = s"${recordsPath}/${jobName}/${outputFilePath}".replaceAll("//", "/")

    griffinLogger.info(s"Writing dataset to csv file: ${path}")
    val columns = dataset.columns
    dataset
      .select(
        columns
          .filter(c => !(c.endsWith(ConstantColumns.tmst) || c.endsWith(Measure.Status)))
          .map(col): _*)
      .write
      .options(Map("header" -> "true", "delimiter" -> delimiter))
      .csv(path)
  }

  private def redisInsert(metrics: Map[String, Any]): Unit = {
    var jedis: Jedis = null
    try {
      jedis = RedisConnection.getResource
      val measureName = metrics.getOrElse(Measure.MeasureName, Unknown)
      val key = s"$jobName/$measureName"
      jedis.sadd(key, JsonUtil.toJson(metrics))
    } catch {
      case e: Throwable => error(e.getMessage, e)
    } finally {
      if (jedis != null) {
        try {
          jedis.close()
        } catch {
          case e: Throwable => error(e.getMessage, e)
        }
      }
    }
  }
}

object RedisConnection {

  case class RedisConf(url: String) {
    def available: Boolean = url.nonEmpty
  }

  val Url = "url";

  private var initialed = false;

  var dataConf: RedisConf = _

  var jedisPool: JedisPool = _

  def init(config: Map[String, Any]): Unit = {
    if (!initialed) {
      dataConf = redisConf(config)
      jedisPool = new JedisPool(dataConf.url)
      initialed = true
    }
  }

  def getResource: Jedis = jedisPool.getResource

  private def redisConf(cfg: Map[String, Any]): RedisConf = {
    val url = cfg.getString(Url, "").trim
    RedisConf(url)
  }

}
