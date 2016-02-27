#!/bin/bash
# https://archive.ics.uci.edu/ml/datasets/PAMAP2+Physical+Activity+Monitoring

rm -f subject.dat subject.binary.dat > /dev/null 2>&1
for i in `seq 101 1 109`; do
  echo "Merging subject$i.dat"
  cat subject$i.dat | sed -e "s/NaN/0/g" -e "/\(^[^\s]*\s\)0\s\(.*\)/d" -e "s/ /,/g" -e "s/\(^[^,]*,\)\([^,]*\),\(.*\)/\1\3,\2/g" >> subject.dat
done
classzero=(1 2 3 9 10 11 17 19)
classone=(4 5 6 7 12 13 16 18 20 24)
cp subject.dat subject.binary.dat
for id in ${classzero[@]}; do
  echo "Replacing target class $id with 0."
  sed -i -e "s/\(^.*,\)$id$/\10/g" subject.binary.dat
done
for id in ${classone[@]}; do
  echo "Replacing target class $id with 1."
  sed -i -e "s/\(^.*,\)$id$/\11/g" subject.binary.dat
done
echo "Done."

# activity ids:
# 1 lying
# 2 sitting
# 3 standing
# 4 walking
# 5 running
# 6 cycling
# 7 Nordic walking
# 9 watching TV
# 10 computer work
# 11 car driving
# 12 ascending stairs
# 13 descending stairs
# 16 vacuum cleaning
# 17 ironing
# 18 folding laundry
# 19 house cleaning
# 20 playing soccer
# 24 rope jumping
# 0 other (transient activities)
# Merge:
# resting: 1 2 3 9 10 11 17 19
# sporting: 4 5 6 7 12 13 16 18 20 24

# Vertica DDL:
# CREATE TABLE activity (x1 float, x2 float, x3 float, x4 float, x5 float, x6 float, x7 float, x8 float, x9 float, x10 float, x11 float, x12 float, x13 float, x14 float, x15 float, x16 float, x17 float, x18 float, x19 float, x20 float, x21 float, x22 float, x23 float, x24 float, x25 float, x26 float, x27 float, x28 float, x29 float, x30 float, x31 float, x32 float, x33 float, x34 float, x35 float, x36 float, x37 float, x38 float, x39 float, x40 float, x41 float, x42 float, x43 float, x44 float, x45 float, x46 float, x47 float, x48 float, x49 float, x50 float, x51 float, x52 float, x53 float, y float);
# CREATE TABLE activity_binary (x1 float, x2 float, x3 float, x4 float, x5 float, x6 float, x7 float, x8 float, x9 float, x10 float, x11 float, x12 float, x13 float, x14 float, x15 float, x16 float, x17 float, x18 float, x19 float, x20 float, x21 float, x22 float, x23 float, x24 float, x25 float, x26 float, x27 float, x28 float, x29 float, x30 float, x31 float, x32 float, x33 float, x34 float, x35 float, x36 float, x37 float, x38 float, x39 float, x40 float, x41 float, x42 float, x43 float, x44 float, x45 float, x46 float, x47 float, x48 float, x49 float, x50 float, x51 float, x52 float, x53 float, y float);

# Load:
# copy activity from '/home/vertica/dataset/activity.dat' with DELIMITER AS ',' skip 0;
# copy activity_binary from '/home/vertica/dataset/activity_binary.dat' with DELIMITER AS ',' skip 0;

# Check
# SELECT y, COUNT(y) FROM activity GROUP BY y ORDER BY y;
# SELECT y, COUNT(y) FROM activity_binary GROUP BY y;

# vertica=> SELECT y, COUNT(y) FROM activity_binary GROUP BY y;
#  y | COUNT 
# ---+-------
#  0 | 64660
#  1 | 88248
# (2 rows)

# vertica=> SELECT y, COUNT(y) FROM activity GROUP BY y ORDER BY y;
#  y  | COUNT 
# ----+-------
#   1 | 15090
#   2 | 14736
#   3 | 15285
#   4 | 19037
#   5 |  6770
#   6 | 12786
#   7 | 15032
#  12 |  9248
#  13 |  8223
#  16 | 13908
#  17 | 19549
#  24 |  3244
# (12 rows)

