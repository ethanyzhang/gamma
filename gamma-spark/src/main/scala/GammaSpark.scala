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
 * @file GammaSpark.scala
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief The Gamma operator programmed in Spark (Scala)
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg._
import org.apache.spark.api.java.JavaSparkContext

object GammaSpark {

  def Gamma(sc: JavaSparkContext, filePath: String): Array[Double] = {
    val Z = sc.sc.textFile(filePath).map { line =>
      val values = line.split(',').map(_.toDouble)
      Vectors.dense(Array[Double](1.0) ++ values)
    }
    val matZ = new RowMatrix(Z)
    return matZ.computeGramianMatrix().toArray
  }

  def GammaMatrixDemo(sc: SparkContext, filePath: String,
                      includesY: Boolean) {
    val Z = sc.textFile(filePath).map { line =>
      val values = line.split(',').map(_.toDouble)
      Vectors.dense(Array[Double](1.0) ++ values)
    }
    val matZ = new RowMatrix(Z)
    val Gamma = new GammaMatrix(matZ.numCols.toInt, matZ.numCols.toInt,
                                matZ.computeGramianMatrix().toArray, includesY)
    println("Gamma:\n" + Gamma)
    println("n="+Gamma.n)
    println("L[3]="+Gamma.L(3))
    println("Q[3,3]="+Gamma.Q(3,3))
    println("Corr: \n" + Gamma.corr)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GammaSpark")
    val sc = new SparkContext(conf)
    // val filePath: String = ("hdfs://node1:54310/KDDn100Kd38_Y.csv")
    val filePath: String = args(0)
    GammaMatrixDemo(sc, filePath, false)
    sc.stop()
  }
}
