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
