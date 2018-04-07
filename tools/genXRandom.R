#!/usr/bin/env Rscript
# Copyright (C) 2013-2017 Yiqun Zhang <contact@yzhang.io>
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

# Get the short form of the n number.
short <- function(num) {
    postfix <- ""
    if (num >= 1000000) {
        postfix = "M"
        num = num / 1000000
    }
    else if (num >= 1000) {
        postfix = "K"
        num = num / 1000
    }
    return(sprintf("%03d%s", num, postfix))
}

# Get command-line arguments.
args <- commandArgs(TRUE)
if (length(args) != 2) {
  stop("genX [n] [d]")
}
n <- as.numeric(args[1])
d <- as.numeric(args[2])
cat("n =", n, "d =", d, "\n")

# Prepare the files.
fileName <- sprintf("n%sd%02d", short(n), d)
fileNameForXCSV = paste("x-",fileName,".csv", sep="")
fileNameForXBinary = paste("x-",fileName,".bin", sep="")
if (file.exists(fileNameForXCSV)) {
    file.remove(fileNameForXCSV);
}
if (file.exists(fileNameForXBinary)) {
    file.remove(fileNameForXBinary);
}
fCSV <- file(fileNameForXCSV, "w");
fBin <- file(fileNameForXBinary, "wb");

# Generate data points
batchSize <- 1000
if (n < batchSize) {
    stop("n < batchSize")
}
if (n %% batchSize != 0) {
    stop("n %% batchSize != 0")
}
batchCount <- as.integer( n / batchSize )
x <- matrix(nrow=batchSize, ncol=d, 0)
for (batchId in 1:batchCount) {
    for (dim in 1:d) {
        x[,dim] = rnorm(n=batchSize,mean=runif(1, min=0, max=10), sd=10)
    }
    x <- x[sample(batchSize),]
    writeBin(as.vector(t(x)), fBin);
    write.table(round(x, digits=3), file=fCSV, row.names=FALSE, col.names=FALSE, sep=",")
    cat(batchId, "batches finished.\n")
}

# To read the binary matrix back:
# matrixD = 100
# matrixN = 1000
# fBin <- file("x-n001Kd100.bin", "rb");
# x=readBin(fBin, numeric(), n=matrixN*matrixD, size=8)
# m = matrix(nrow=matrixN, byrow=TRUE, data=x)
