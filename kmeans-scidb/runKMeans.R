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

library('scidb')
library('GammaR')
scidbconnect()
# query="kmeans(n001Md8k8, 8, 50, 'incremental=true,omega=0.5')"
# kmodel = iquery(query, return = TRUE)
run <- function() {
  cat("1: incremental, omega=0.5\n")
  cat("2: incremental, omega=0\n")
  cat("3: non-incremental, omega=0.5\n")
  cat("4: non-incremental, omega=0\n")
  cat("n\td\tk\ttime(1)\tavgD(1)\ttime(2)\tavgD(2)\ttime(3)\tavgD(3)\ttime(4)\tavgD(4)\n")
  n = 1000000
  while (n <= 10000000) {
    d = 8
    k = 4
    while (k <= 16) {
      arrName = sprintf("n%sd%dk%d", short(n), d, k)
      t1 = round(system.time(kmodel1 <- KMeans(arrName, k, 50, TRUE, 0.5))[3],3)
      Sys.sleep(1)
      t2 = round(system.time(kmodel2 <- KMeans(arrName, k, 50, TRUE, 0))[3],3)
      Sys.sleep(1)
      t3 = round(system.time(kmodel3 <- KMeans(arrName, k, 50, FALSE, 0.5))[3],3)
      Sys.sleep(1)
      t4 = round(system.time(kmodel4 <- KMeans(arrName, k, 50, FALSE, 0))[3],3)
      Sys.sleep(1)
      cat(n, "\t", d, "\t", k, "\t", sep="")
      cat(t1, "\t", round(kmodel1$avgDistance,3), "\t", sep="")
      cat(t2, "\t", round(kmodel2$avgDistance,3), "\t", sep="")
      cat(t3, "\t", round(kmodel3$avgDistance,3), "\t", sep="")
      cat(t4, "\t", round(kmodel4$avgDistance,3), "\n", sep="")
      k = k * 2
    }
    n = n * 10
  }
}
