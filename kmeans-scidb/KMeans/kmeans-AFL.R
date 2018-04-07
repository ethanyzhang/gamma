library('scidb')
scidbconnect('129.7.243.232', port=8083L)
set.seed(1)
n=400
d=20
k=10
nperk=as.numeric(n/k);
means=matrix(nrow=k,runif(k*d, min=0, max=200)[sample(k*d)]);
x=NULL;
for (i in 1:k) {
  xk=NULL;
  for (j in 1:d) {
    xk=rbind(xk, rnorm(n=nperk,mean=means[i,j], sd=10));
  }
  x=cbind(x, xk);
}
x=x[,sample(n)];

x = as.scidb(t(x))
t = system.time(k <- kmeans(x, 10, 10))

x=scidb('x4k')
x = project(x, scidb_attributes(x)[1])
x = attribute_rename(x,new="val")
x = dimension_rename(x,new=c("i","j"))
expr = sprintf("random() %% %d", k)

t = system.time(k <- kmeans(data, 10, 10))
# store(aggregate(apply(cross_join(cast(cast(project(x4k,val), <val:double>[i=0:3999,4000,0,j=0:19,20,0]), <val:double>[i=0:3999,4000,0,j=0:19,20,0]) as x, cast(R_arrayf265543a5871101020794246,<val_avg:double NULL>[group=0:*,8,0,j=0:19,20,0]) as y,x.j,y.j), dist,(val - val_avg)*(val - val_avg)), sum(dist) as dist, i,group),repart(R_arrayf26316b67fb1101020794246, <dist:double NULL DEFAULT null> [i=0:3999,4000,0,group=0:*,8,0]))
