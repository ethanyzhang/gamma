#!/bin/bash
n=("000100K" "000001M" "000010M" "000100M")
d=("000010" "000100")
len_n=${#n[@]}
len_d=${#d[@]}

for ((i=0; i<len_n; i++)); do
    for ((j=0; j<len_d; j++)); do
        for type in "dense" "sparse"; do
            dataset="scidb.KDDnet_n${n[$i]}_d${d[$j]}_${type}"
            echo "Running on ${dataset}"
            ./vcrossprod.sh ${dataset}
        done
    done
done
