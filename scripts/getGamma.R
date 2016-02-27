getGamma = function(arrName) {
  strquery = paste("DenseGamma(", arrName, ");", sep="")
  Gamma_vertical = iquery(strquery, return=TRUE)
  d = max(Gamma_vertical$i)
  Gamma = matrix(0, nrow=d, ncol=d)
  k = 1
  for (i in 1:d)
  for (j in 1:d) {
    Gamma[i,j] = Gamma_vertical[k,3]
    k = k+1
  }
  return(Gamma)
}
