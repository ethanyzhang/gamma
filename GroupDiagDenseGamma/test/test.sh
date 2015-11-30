#!/bin/bash
# This script tests the GroupDiagDenseGamma operator.
port=1239
echo "Port number is set to $port."

echo -e "\e[1;32mCleaning up old arrays.\e[0m"
iquery -anp $port -q "remove(GroupDiagGammaTestXImport);" --ignore-errors
iquery -anp $port -q "remove(GroupDiagGammaTestXOneChunk);" --ignore-errors
iquery -anp $port -q "remove(GroupDiagGammaTestXTwoChunks);" --ignore-errors
iquery -anp $port -q "remove(GroupDiagGammaTestGroupOneChunk);" --ignore-errors
iquery -anp $port -q "remove(GroupDiagGammaTestGroupTwoChunks);" --ignore-errors

echo -e "\e[1;32mCreating arrays.\e[0m"
iquery -anp $port -q "create array GroupDiagGammaTestXImport <i:int64,j:int64,val:double> [x=1:16,16,0];"
iquery -anp $port -q "create array GroupDiagGammaTestXOneChunk <val:double> [i=1:4,4,0,j=1:4,4,0];"
iquery -anp $port -q "create array GroupDiagGammaTestXTwoChunks <val:double> [i=1:4,2,0,j=1:4,4,0];"
iquery -anp $port -q "create array GroupDiagGammaTestGroupOneChunk <g:int64> [i=1:4,4,0];"
iquery -anp $port -q "create array GroupDiagGammaTestGroupTwoChunks <g:int64> [i=1:4,2,0];"

echo -e "\e[1;32mImporting data.\e[0m"
if [ ! -f XV.csv ] || [ ! -f group.csv ]; then
	echo "XV.csv or group.csv not exist."
	exit 1
fi
loadcsv.py -p $port -a 'GroupDiagGammaTestXImport' -i XV.csv -c 16 -f 1
loadcsv.py -p $port -a 'GroupDiagGammaTestGroupOneChunk' -i group.csv -c 4 -f 1
loadcsv.py -p $port -a 'GroupDiagGammaTestGroupTwoChunks' -i group.csv -c 2 -f 1
iquery -anp $port -q "store(redimension(GroupDiagGammaTestXImport, GroupDiagGammaTestXOneChunk), GroupDiagGammaTestXOneChunk);"
iquery -anp $port -q "store(redimension(GroupDiagGammaTestXImport, GroupDiagGammaTestXTwoChunks), GroupDiagGammaTestXTwoChunks);"

tests=( 
       "GroupDiagDenseGamma(GroupDiagGammaTestXOneChunk, GroupDiagGammaTestGroupOneChunk);"
       "GroupDiagDenseGamma(GroupDiagGammaTestXTwoChunks, GroupDiagGammaTestGroupTwoChunks);"
       "GroupDiagDenseGamma(GroupDiagGammaTestXOneChunk, GroupDiagGammaTestGroupTwoChunks);"
       "GroupDiagDenseGamma(GroupDiagGammaTestXOneChunk, filter(GroupDiagGammaTestGroupOneChunk, i<10) );" 
       )
for ((i=0; i<${#tests[@]}; i++)); do
	echo -e "\e[1;32mTesting \e[0m\e[1;33m${tests[i]}\e[0m"
	iquery -ap $port -q "${tests[i]}"
done