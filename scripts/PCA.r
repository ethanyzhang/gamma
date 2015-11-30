# Copyright (C) 2015 UH DBMS group
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

# PCA (Pricipal component Analysis)

getGamma = function(arrName) {
  strquery = paste("DenseGamma(", arrName, ");", sep="")
  Gamma_ijv = iquery(strquery, return=TRUE)
  d = max(Gamma_ijv$i)
  Gamma = matrix(0, nrow=d, ncol=d)
  k = 1
  for (i in 1:d)
  for (j in 1:d) {
    Gamma[i,j] = Gamma_ijv[k,3]
    k = k+1
  }
  return(Gamma)
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

timePCA = function(arrName, d) {
  tGamma = system.time( Gamma <- getGamma( arrName ) )[3]
  tPCA = system.time( pcaResult <- getPCA( Gamma, d ) )[3]
  cat(arrName, ": Gamma took ", tGamma, " seconds, PCA took ", tGamma+tPCA, " seconds in total.\n", sep="")
}

library(scidb)
scidbconnect()
timePCA("kdd1m", 38)
timePCA("kdd10m", 38)
timePCA("kdd100m", 38)
