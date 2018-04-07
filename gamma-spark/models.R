# Copyright (C) 2013-2016 Yiqun Zhang <zhangyiqun9164@gmail.com>
# All Rights Reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# Linear regression and PCA in R using Spark.

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/usr/local/spark")
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sc <- sparkR.init(master = "spark://node1:7077", sparkEnvir = list(spark.executor.memory="2g"), sparkJars=c("gammaspark_2.10-1.0.jar"))

getGamma = function(sc, filePath) {
  GammaByte <- SparkR:::callJStatic("GammaSpark", "Gamma", sc, filePath)
  Gamma <- matrix(unlist(GammaByte), ncol=sqrt(length(GammaByte)))
  return(Gamma)
}

getLR = function(Gamma, d) {
  L = Gamma[2:(d+1),1];
  Var  = Gamma[2:(d+1), 2:(d+1)]/Gamma[1,1] - ( L%*% t(L) )/Gamma[1,1]^2  
  idx = which(diag(Var) > 0.5 )
  idx = c(1, idx+1, d+2)
  GammaSel = Gamma[idx, idx]
  idxlen = length(idx)
  Bet=solve(GammaSel[1:(idxlen-1),1:(idxlen-1)])%*%(GammaSel[1:(idxlen-1),idxlen])
  return(Bet)
}

corrMat = function(n,L,Q,d) {
   rho = Q
   for (a in 1:d) {
       for (b in 1:d) {
          rho[a,b] = (n*Q[a,b]- L[a]*L[b])/
      ( sqrt(n*Q[a,a]-L[a]*L[a]) * 
      ( sqrt(n*Q[b,b]-L[b]*L[b])) )
    }
  }
  return(rho)
}

getPCA = function(Gamma, d) {
  n = Gamma[1,1]
  Q = Gamma[2:(d+1),2:(d+1)]
  L = Gamma[2:(d+1),1]
  Rho = corrMat(n,L,Q,d)
  return(svd(Rho))
}

timeForGamma = function(sc, filePath) {
  tGamma = system.time( Gamma <- getGamma( sc, filePath ) )[3]
  cat(filePath, ":\nGamma: ", tGamma, " seconds.\n", sep="")
}

timeForDataset = function(sc, filePath, d) {
  tGamma = system.time( Gamma <- getGamma( sc, filePath ) )[3]
  tPCA = system.time( pcaResult <- getPCA( Gamma, d ) )[3]
  tLR = system.time( lrResult <- getLR( Gamma, d ) )[3]
  cat(filePath, ":\nGamma: ", tGamma, " seconds.\n", sep="")
  cat("PCA: ", tGamma+tPCA, " seconds in total.\n", sep="")
  cat("LR: ", tGamma+tLR, " seconds in total.\n", sep="")
}

# hadoop fs -D dfs.block.size=6291456 -put kdd100kd020.csv /
timeForGamma(sc, "hdfs://node1:54310/kdd100kd020.csv")

timeForGamma(sc, "hdfs://node1:54310/KDDn100Kd38_Y.csv")
timeForGamma(sc, "hdfs://node1:54310/KDDn001Md38_Y.csv")
timeForDataset(sc, "hdfs://node1:54310/KDDn010Md38_Y.csv", 38)
timeForDataset(sc, "hdfs://node1:54310/KDDn100Md38_Y.csv", 38)
