CXX = g++
RM = rm -f
CXXFLAGS = -Wall -std=c++11

all: tab2vert csv2binary

csv2binary: csv2binary.cpp
	$(CXX) $(CXXFLAGS) -o csv2binary csv2binary.cpp

tab2vert: tab2vert.cpp
	$(CXX) $(CXXFLAGS) -o tab2vert tab2vert.cpp

clean:
	$(RM) tab2vert
	$(RM) csv2binary
