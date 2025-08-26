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
