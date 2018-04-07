/*
 * Copyright (C) 2013-2016 Yiqun Zhang <zhangyiqun9164@gmail.com>
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdint.h>
#define N 80000000

double pi(bool usegpu) {
  double pi = 0.0;
  #pragma acc enter data copyin(pi) if (usegpu)
  #pragma acc kernels loop present(pi) reduction(+:pi) if (usegpu)
  for (int64_t i=0; i<N; i++) {
    double t = (double)((i+0.5)/N);
    pi += 4.0/(1.0+t*t);
  }
  #pragma acc exit data copyout(pi) if (usegpu)
  return pi/N;
}
