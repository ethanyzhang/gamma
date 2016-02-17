parseVectors <-  function(lines) {
  lines <- strsplit(as.character(lines) , ',', fixed = TRUE)
  matrix(as.numeric(unlist(lines)), ncol = length(lines[[1]]))
  # list(matrix(as.numeric(unlist(lines)), ncol = length(lines[[1]])))
}

lines = SparkR:::textFile(sc, "hdfs://node1:54310/KDDn100Kd38_Y.csv")
X = SparkR:::lapply(lines, parseVectors)
x1 = take(X, 1)
# partitionedXs = SparkR:::lapplyPartition(lines, parseVectors)
