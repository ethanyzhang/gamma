#!/bin/bash
if [ $# -ne 1 ]; then
    echo "Usage: vcrossprod.sh [table name]"
    echo "Example: vcrossprod.sh table"
    exit
fi

password="2347Sunvalley"
rm vcrossprod.sql > /dev/null 2>&1
echo "\timing" >> vcrossprod.sql
echo "SELECT a.i AS i, b.j AS j, sum(a.v * b.v) AS v FROM $1 a JOIN $1 b ON a.j = b.i GROUP BY a.i, b.j;" >> vcrossprod.sql
vsql -w $password -f vcrossprod.sql -o /dev/null
rm vcrossprod.sql > /dev/null 2>&1
