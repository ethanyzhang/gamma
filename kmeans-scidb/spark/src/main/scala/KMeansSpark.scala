/*
 * Copyright (C) 2013-2016 Yiqun Zhang <zhangyiqun9164@gmail.com>
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * @file KMeansSpark.scala
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief The KMeans operator programmed in Spark (Scala)
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}


object KMeansSpark {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("KMeansSpark")
    val sc = new SparkContext(conf)
    val filePath: String = args(0)

    // Load and parse the data
    val data = sc.textFile(filePath)
    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 10
    val numIterations = 50
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    // val WSSSE = clusters.computeCost(parsedData)
    // println("Within Set Sum of Squared Errors = " + WSSSE)
    // println(clusters.clusterCenters())

    sc.stop()
  }
}
