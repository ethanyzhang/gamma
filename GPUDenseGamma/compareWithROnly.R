Gamma = function(x) {
    nrow = dim(x)[1];
    x = cbind(as.matrix(rep(1, nrow), n=nrow), x);
    gamma = t(x) %*% x;
    return(gamma)
}

matrixD = 100
matrixN = 1000000
fBin <- file("x-n010Md100.bin", "rb");
x = readBin(fBin, numeric(), n=matrixN*matrixD, size=8)
m = matrix(nrow=matrixN, byrow=TRUE, data=x)
# system.time(gamma <- Gamma(m))
# system.time(gamma <- princomp(m))
system.time(gamma <- lm(m[,matrixD] ~ m[,1:matrixD-1]))