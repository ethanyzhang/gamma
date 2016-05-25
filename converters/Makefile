CXX = g++
RM = rm -f
CXXFLAGS = -Wall -std=c++11
TARGET = tab2vert vert2tab csv2binary

all: $(TARGET)

csv2binary: csv2binary.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^

tab2vert: tab2vert.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^

vert2tab: vert2tab.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	$(RM) $(TARGET)
