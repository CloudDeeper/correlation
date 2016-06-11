#!/usr/bin/bash
keyword="Mean"
name="/home/cp2016/s104062573/data_collection/res"
ARRAY=()

while IFS=',' read -ra line
	do
	ARRAY+=("${line[1]}")
done < "$name"

for filename in "$@"
do
k=0
d=1
	while IFS=',' read -ra line
	do
isFind=0
for i in "${ARRAY[@]}"
do
    if [ "$i" == "${line[2]}" ] 
		then
		let "isFind = 1"
		break
    fi
done
	if [ $isFind == 0 ]
	then	
		IFS=','
		echo "${line[*]}"
	fi
	done < "$filename"
done


#./parser.sh csv/*
