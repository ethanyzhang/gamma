import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vectors

val filePath: String = ("hdfs://node1:54310/KDDn001Md38_Y.csv")
val rows = sc.textFile(filePath).map { line =>
  val values = line.split(',').map(_.toDouble)
  Vectors.dense(values)
}
val mat = new RowMatrix(rows)
val Gamma = mat.computeGramianMatrix()
println("Gamma:\n" + Gamma)