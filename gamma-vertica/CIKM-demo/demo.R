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

options(digits=3)
options(width=200)
library('RJDBC')
DenseGamma <- function(tableName, d, postgresql = FALSE) {
  query = ""
  if (postgresql) {
    query = paste("SELECT a.j AS i, b.j AS j, sum(a.v * b.v) AS v FROM ", tableName, " a JOIN ", tableName, " b ON a.i = b.i GROUP BY a.j, b.j;", sep="")
  }
  else {
    query = paste("SELECT DenseGamma(i, j, v USING PARAMETERS d=", d, ") OVER (PARTITION BY MOD(i,4) ORDER BY i,j) FROM ", tableName, ";", sep="")
  }
  res = dbGetQuery(conn, query)
  Gamma = matrix(0, nrow=d+1, ncol=d+1)
  k = 1
  for (i in 1:(d+1))
  for (j in 1:(d+1)) {
    Gamma[i, j] = res[k, 3]
    k = k + 1
  }
  return(Gamma)
}

getLR = function(Gamma, d) {
  L = Gamma[2:(d+1),1];
  Var = Gamma[2:(d+1), 2:(d+1)]/Gamma[1,1] - ( L%*% t(L) )/Gamma[1,1]^2  
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

getGammaUsingSpark = function(sc, filePath) {
  GammaByte <- SparkR:::callJStatic("GammaSpark", "Gamma", sc, filePath)
  Gamma <- matrix(unlist(GammaByte), ncol=sqrt(length(GammaByte)))
  return(Gamma)
}


className = "com.vertica.jdbc.Driver"
classPath = "vertica-jdbc-7.2.3-0.jar"
jdbcConnectionString = "jdbc:vertica://node1/gamma"
userName = "vertica"
password = ""
tableName = "kddnet"
d = 9
drv <- JDBC(className, classPath, identifier.quote="`")
conn <- dbConnect(drv, jdbcConnectionString, userName, password)
Gamma = DenseGamma(tableName, d+1)
getPCA(Gamma, d+1)
getLR(Gamma, d)
system.time(Gamma <- DenseGamma(tableName, d+1))[3]

password = "12512Marlive"
className = "org.postgresql.Driver"
classPath = "postgresql-9.4.1208.jar"
jdbcConnectionString = "jdbc:postgresql://node1/vertica"
drv <- JDBC(className, classPath, identifier.quote="`")
conn <- dbConnect(drv, jdbcConnectionString, userName, password)
system.time(Gamma <- DenseGamma(tableName, d+1, postgresql = TRUE))[3]

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sc <- sparkR.init(master = "local[1]", sparkEnvir = list(spark.executor.memory="4g"), sparkJars=c("gammaspark_2.10-1.0.jar"))
filePath="hdfs://node1:54310/j-gamma/demo.csv"
system.time(Gamma <- getGammaUsingSpark(sc, filePath))[3]

tableName = "kddnet"
k = 4
imax = 50
incremental = "true"
omega = 0.5
if (is.na(incremental)) {
  incremental = "false"
} else if (incremental != "true") {
  incremental = "false"
}
if (is.na(omega)) {
  omega = 0
}
cat("dataset =", tableName, "\nk = ", k, "\nimax = ", imax, "\n")
cat("incremental = ", incremental, "\nomega = ", omega, "\n");
query = paste("kmeans(", tableName, ", ", k, ", ", imax, ", 'incremental=", incremental, ",omega=", omega, "')", sep="")
cat(query, "\n")

library('scidb')
scidbconnect('129.7.243.232', port=8083L)

kmodel=iquery(query, return=TRUE)
d=(dim(kmodel)[1]/k-1)/2
W = matrix(0, nrow=k, ncol=1)
C = matrix(0, nrow=k, ncol=d)
R = matrix(0, nrow=k, ncol=d)
cur=1
for (i in 1:k) {
  W[i] = kmodel[cur,3]
  cur=cur+1
  for (j in 1:d) {
    C[i,j] = kmodel[cur,3]
    cur=cur+1
  }
  for (j in 1:d) {
    R[i,j] = kmodel[cur,3]
    cur=cur+1
  }
}
cat("W:\n")
print.table(W)
cat("\nC:\n")
print.table(C)
cat("\nR:\n")
print.table(R)
