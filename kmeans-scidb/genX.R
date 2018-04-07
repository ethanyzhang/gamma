#!/usr/bin/env Rscript
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
if (length(args) != 3) {
  stop("genX [n] [d] [k]")
}
n <- as.numeric(args[1])
d <- as.numeric(args[2])
k <- as.numeric(args[3])
cat("n =", n, "d =", d, "k =", k, "\n")

# Prepare the files.
fileName <- sprintf("n%sd%02dk%02d", short(n), d, k)
fileNameForCenters <- paste("c-",fileName,".csv", sep="")
fileNameForXCSV = paste("x-",fileName,".csv", sep="")
fileNameForXBinary = paste("x-",fileName,".bin", sep="")
if (file.exists(fileNameForCenters)) {
    file.remove(fileNameForCenters);
}
if (file.exists(fileNameForXCSV)) {
    file.remove(fileNameForXCSV);
}
if (file.exists(fileNameForXBinary)) {
    file.remove(fileNameForXBinary);
}
fCenter <- file(fileNameForCenters, "w")
fCSV <- file(fileNameForXCSV, "w");
fBin <- file(fileNameForXBinary, "wb");

# Generate the centers
means <- matrix(nrow=k,runif(k*d, min=0, max=10)[sample(k*d)]);
write.table(round(means, digits=3), file=fCenter, row.names=FALSE, col.names=FALSE, sep=",")

# Generate data points
batchSize <- 1000
if (n < batchSize) {
    stop("n < batchSize")
}
if (n %% batchSize != 0) {
    stop("n %% batchSize != 0")
}
batchCount <- as.integer( n / batchSize )
pointPerK <- as.integer( batchSize / k )
x <- matrix(nrow=batchSize, ncol=d, 0)
for (batchId in 1:batchCount) {
    for (cluster in 1:k) {
        start <- pointPerK * (cluster - 1) + 1
        end <- pointPerK * cluster
        if (cluster == k) {
            end <- batchSize
        }
        for (dim in 1:d) {
            x[start:end, dim] = rnorm(n=(end-start+1),mean=means[cluster,dim], sd=10)
        }
    }
    x <- x[sample(batchSize),]
    writeBin(as.vector(t(x)), fBin);
    write.table(round(x, digits=3), file=fCSV, row.names=FALSE, col.names=FALSE, sep=",")
}
