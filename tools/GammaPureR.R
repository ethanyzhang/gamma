x = as.matrix(read.csv('cbm.csv', header=FALSE));
nrow = dim(x)[1];
x = cbind(as.matrix(rep(1, nrow), n=nrow), x);
Gamma = t(x) %*% x;
