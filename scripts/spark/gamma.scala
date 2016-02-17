import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vectors

val filePath: String = ("hdfs://node1:54310/KDDn100Kd38_Y.csv")
val Z = sc.textFile(filePath).map { line =>
  val values = line.split(',').map(_.toDouble)
  Vectors.dense(Array[Double](1.0) ++ values)
}
val matZ = new RowMatrix(Z)
val Gamma = matZ.computeGramianMatrix()
println("Gamma:\n" + Gamma) 