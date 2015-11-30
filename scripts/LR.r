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

# LR (Linear Regression)

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

timeLR = function(arrName, d) {
  tGamma = system.time( Gamma <- getGamma( arrName ) )[3]
  tLR = system.time( lrResult <- getLR( Gamma, d ) )[3]
  cat(arrName, ": Gamma took ", tGamma, " seconds, LR took ", tGamma+tLR, " seconds in total.\n", sep="")
}

library(scidb)
scidbconnect()
timeLR("kdd1m", 38)
timeLR("kdd10m", 38)
timeLR("kdd100m", 38)
