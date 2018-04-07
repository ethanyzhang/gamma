Gram <- function(filename) {
  X = as.matrix(read.csv(filename, header=FALSE));
  G = t(X) %*% X;
}

cat(system.time(Gram('kdd010kd010.csv'))[3], "\n");
cat(system.time(Gram('kdd010kd020.csv'))[3], "\n");
cat(system.time(Gram('kdd010kd040.csv'))[3], "\n");
cat(system.time(Gram('kdd010kd080.csv'))[3], "\n");
cat(system.time(Gram('kdd100kd010.csv'))[3], "\n");
cat(system.time(Gram('kdd100kd020.csv'))[3], "\n");
cat(system.time(Gram('kdd100kd040.csv'))[3], "\n");
cat(system.time(Gram('kdd100kd080.csv'))[3], "\n");
cat(system.time(Gram('kdd001md010.csv'))[3], "\n");
cat(system.time(Gram('kdd001md020.csv'))[3], "\n");
cat(system.time(Gram('kdd001md040.csv'))[3], "\n");
cat(system.time(Gram('kdd001md080.csv'))[3], "\n");
