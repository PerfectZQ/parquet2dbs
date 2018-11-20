package com.zq.elasticsearch

import java.util.Collections

import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.{HttpEntity, HttpHost}
import org.elasticsearch.client.{Response, RestClient}
import java.util.{Map => JavaMap}

class ElasticSearch(url: String, port: Int) extends Serializable {

  // 目前支持的分解器，后续有需要还会添加
  private val TOKENIZERS: Set[String] = Set("ngram")

  /**
    * 配置时间类型的自动推断
    * 后缀为`__date`的字段将后转换成`date`类型
    *
    * @param indexName 索引名称
    * @return 操作是否成功
    */
  def configureIndexWithDateMatch(indexName: String): Boolean = {
    val params = Collections.emptyMap[String, String]()
    val body: String =
      s"""
         |{
         |    "dynamic_templates": [
         |        {
         |            "date_match": {
         |                "match": "*__date",
         |                "mapping": {
         |                    "type": "date"
         |                }
         |            }
         |        }
         |    ]
         |}""".stripMargin

    executeDSL("PUT", s"$indexName/_mapping/$indexName", params, body)
  }

  /**
    * 创建索引
    *
    * @param indexName
    * @return 操作是否成功
    */
  def createIndex(indexName: String): Boolean = {
    val params = Collections.emptyMap[String, String]()
    executeDSL("PUT", indexName, params, "")
  }

  /**
    * 删除索引
    *
    * @param indexName
    * @return 操作是否成功
    */
  def deleteIndex(indexName: String): Boolean = {
    val params = Collections.emptyMap[String, String]()
    executeDSL("DELETE", indexName, params, "")
  }

  /**
    * 配置字段分词器
    * 访问分词器字段: $field.$analyser
    *
    * @param indexName 索引名称
    * @param field     字段名
    * @param analyser  分词器
    * @return 操作是否成功
    */
  def configureAnalyser(indexName: String, field: String, analyser: String): Boolean = {
    val params = Collections.emptyMap[String, String]()
    if (TOKENIZERS.contains(analyser)) registerTokenizer(indexName, analyser)
    val body =
      s"""
         |{
         |    "properties": {
         |      "$field": {
         |        "type": "text",
         |        "fields": {
         |          "keyword": {
         |            "type": "keyword"
         |          },
         |          "$analyser":{
         |            "type": "text",
         |            "analyzer": "$analyser"
         |          }
         |        }
         |      }
         |    }
         |}""".stripMargin

    executeDSL("PUT", s"$indexName/_mapping/$indexName", params, body)
  }

  /**
    * 注册 Tokenizer 为 Analyzer
    *
    * @param indexName
    * @param tokenizer
    * @return 操作是否成功
    */
  def registerTokenizer(indexName: String, tokenizer: String): Boolean = {
    val params = Collections.emptyMap[String, String]()
    var state = false
    if (closeIndex(indexName)) {
      val body =
        s"""
           |{
           |    "analysis": {
           |      "analyzer": {
           |        "$tokenizer": {
           |          "tokenizer": "$tokenizer"
           |        }
           |      },
           |      "tokenizer": {
           |        "$tokenizer": {
           |          "type": "$tokenizer"
           |        }
           |      }
           |    }
           |}""".stripMargin
      state = executeDSL("PUT", s"$indexName/_settings", params, body)
      if (!openIndex(indexName)) throw new RuntimeException("打开索引失败")
    } else {
      throw new RuntimeException("关闭索引失败")
    }
    state
  }

  /**
    * 索引是否已经存在
    *
    * @param indexName
    * @return 索引是否存在
    */
  def indexExists(indexName: String): Boolean = {
    val params = Collections.emptyMap[String, String]()
    // executeDSL("GET", s"_cat/indices/$indexName", params, "", url, port)
    executeDSL("HEAD", s"$indexName", params, "")
  }

  /**
    * 查看记录是否已经存在
    *
    * @param indexName
    * @return 记录是否已经存在
    */
  def documentExists(restClient: RestClient, indexName: String, id: String): Boolean = {
    val params = Collections.emptyMap[String, String]()
    executeDSLWithoutClient("HEAD", s"$indexName/$indexName/$id", params, "", restClient)
  }

  def closeIndex(indexName: String): Boolean = {
    val params = Collections.emptyMap[String, String]()
    executeDSL("POST", s"$indexName/_close", params, "")
  }

  def openIndex(indexName: String): Boolean = {
    val params = Collections.emptyMap[String, String]()
    executeDSL("POST", s"$indexName/_open", params, "")
  }

  /** 添加 double 类型字段映射
    *
    * @param indexName
    * @param field
    * @return
    */
  def configureDoubleTypeMapping(indexName: String, field: String): Boolean = {
    val params = Collections.emptyMap[String, String]()
    val body =
      s"""
         |{
         |  "properties": {
         |    "$field": {
         |      "type": "double"
         |    }
         |  }
         |}
     """.stripMargin
    executeDSL("PUT", s"$indexName/_mapping/$indexName", params, body)
  }


  /** 添加 date 类型字段映射
    *
    * @param indexName
    * @param field
    * @return
    */
  def configureDateTypeMapping(indexName: String, field: String): Boolean = {
    val params = Collections.emptyMap[String, String]()
    val body =
      s"""
         |{
         |  "properties": {
         |    "$field": {
         |      "type": "date",
         |      "format": "yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
         |    }
         |  }
         |}
     """.stripMargin
    executeDSL("PUT", s"$indexName/_mapping/$indexName", params, body)
  }


  /** 批量添加 date 类型字段映射
    *
    * @param indexName
    * @param dateFields
    * @return
    */
  def configureDateTypeMapping(indexName: String, dateFields: Seq[String]): Boolean = {
    val params = Collections.emptyMap[String, String]()
    val body = dateFields.map { field =>
      s"""
         |{
         |  "properties": {
         |    "$field": {
         |      "type": "date",
         |      "format": "yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
         |    }
         |  }
         |}
          """.stripMargin
    }.mkString(",")
    executeDSL("PUT", s"$indexName/_mapping/$indexName", params, body)
  }

  /**
    * 获取 Rest Client
    *
    * @param scheme
    * @return 操作是否成功
    */
  def getRestClient(scheme: String = "http"): RestClient = {
    val httpHost = new HttpHost(url, port, scheme)
    RestClient.builder(httpHost).build
  }

  /**
    * 执行 QueryDSL
    *
    * @param method HEAD/GET/DELETE/PUT/POST
    * @param uri    请求路径
    * @param params 参数
    * @param body   请求体
    * @return 操作是否成功
    */
  def executeDSL(method: String,
                 uri: String,
                 params: JavaMap[String, String],
                 body: String,
                 scheme: String = "http"): Boolean = {

    val restClient = getRestClient(scheme)
    var response: Response = null
    try {
      method.toUpperCase() match {
        case x if Set("HEAD", "GET", "DELETE").contains(x) =>
          println(s"""curl -X $method "$scheme://$url:$port/$uri?pretty" """)
          response = restClient.performRequest(method, uri)
        case x if Set("PUT", "POST").contains(x) =>
          val entity: HttpEntity = new NStringEntity(body, ContentType.APPLICATION_JSON)
          println(s"""curl -X $method "$scheme://$url:$port/$uri?pretty" -H "Content-Type: application/json" -d'$body'""")
          response = restClient.performRequest(method, uri, params, entity)
        case x => throw new IllegalArgumentException(s"Unsupported type $x")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      restClient.close()
    }

    response != null && response.getStatusLine.getStatusCode == 200
  }

  /**
    * 执行 QueryDSL，需要自己维护 restClient 对象的创建和关闭
    *
    * @param method HEAD/GET/DELETE/PUT/POST
    * @param uri    请求路径
    * @param params 参数
    * @param body   请求体
    * @param restClient
    * @return 操作是否成功
    */
  def executeDSLWithoutClient(method: String,
                              uri: String,
                              params: JavaMap[String, String],
                              body: String,
                              restClient: RestClient,
                              scheme: String = "http"): Boolean = {
    var response: Response = null
    method.toUpperCase() match {
      case x if Set("HEAD", "GET", "DELETE").contains(x) =>
        println(s"""curl -X $method "$scheme://$url:$port/$uri?pretty" """)
        response = restClient.performRequest(method, uri)
      case x if Set("PUT", "POST").contains(x) =>
        val entity: HttpEntity = new NStringEntity(body, ContentType.APPLICATION_JSON)
        println(s"""curl -X $method "$scheme://$url:$port/$uri?pretty" -H "Content-Type: application/json" -d'$body'""")
        response = restClient.performRequest(method, uri, params, entity)
      case x => throw new IllegalArgumentException(s"Unsupported type $x")
    }
    response != null && response.getStatusLine.getStatusCode == 200
  }

}