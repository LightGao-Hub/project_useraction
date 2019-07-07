package com.gupao.bigdata.spark.demo

import org.eclipse.jetty.util.{MultiMap, UrlEncoded}

object HelloWorld {

  def test(i: Int) = {
    if (i < 0) {
      "a"
    } else {
      1
    }
  }

  def main(args: Array[String]): Unit = {
    /*val f0 : (Int, Int) => Boolean = (i, j) =>  {i % 2 == 0}
    1.to(10)*/
    val paramsMap = new MultiMap[String]
    UrlEncoded decodeTo("http://opencart.com/index.php?route=product/product/review&product_id=40", paramsMap, "UTF-8")
    val productId: String = paramsMap.getValue("product_id", 0);
    println(productId)
  }

}
