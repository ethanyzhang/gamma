getGroupDiagGamma = function(arrName, k, idY=NULL) {
  strquery = paste("GroupDiagDenseGamma(", arrName, ", ", k, sep="")
  if(!is.null(idY))
    strquery = paste(strquery, ", ", idY, sep="")
  strquery = paste(strquery, ")", sep="")
  Gamma_vertical = iquery(strquery, return=TRUE)
  d = (max(Gamma_vertical$j)-3)/2
  Gamma = matrix(0, nrow=k, ncol=2*d+3)
  l=1
  for (i in 1:k)
  for (j in 1:(2*d+3)) {
    Gamma[i,j] = Gamma_vertical[l,3]
    l = l+1
  }
  return(Gamma)
}
