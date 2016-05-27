Gamma = function(arrName) {
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

LR = function(Gamma) {
  d = dim(Gamma)[1]-2
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

PCA = function(Gamma) {
  d = dim(Gamma)[1]-1
  n = Gamma[1,1]
  Q = Gamma[2:(d+1),2:(d+1)]
  L = Gamma[2:(d+1),1]
  Rho = corrMat(n,L,Q,d)
  return(svd(Rho))
}

KMeans = function(dataset, k, imax = 50, incremental = TRUE, omega = 0.5) {
  query = paste("kmeans(", dataset, ", ", k, ", ", imax, ", 'incremental=", incremental, ",omega=", omega, "')", sep="")
  cat(query, "\n")
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
  return(list(W=W,C=C,R=R))
}
