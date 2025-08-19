#!/bin/bash
echo "SUM OF NUMBERS"
echo -n  "enter n:"
read digit
echo "digit is : $digit"
t=1
sum=0
while [ $t -le $digit ]; do
	sum=$((t+sum))
	((t++))
done
echo "SUM : $sum"

echo "FACTORIAL OF N"
echo -n "enter n:"
read -r n
echo "digit is:$digit"
res=1
for i in $(seq 2 $n);do
	res=$((i*res))
done
echo "FACTORIAL : $res"

