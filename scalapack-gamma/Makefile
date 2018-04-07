all:
	mpicc -o scalapack-gamma-c -g -O2 scalapack-gamma.c -lscalapack-openmpi -lblacsCinit-openmpi -lblacs-openmpi -llapack -lblas -lgfortran

	mpic++ -std=c++11 -o scalapack-gamma-cpp -g -O2 scalapack-gamma.cpp -lscalapack-openmpi -lblacsCinit-openmpi -lblacs-openmpi -llapack -lblas -lgfortran

clean:
	rm -rf *~ *# scalapack-gamma-c scalapack-gamma-cpp
