#!/usr/bin/bash
keyword="Mean"
for filename in "$@"
do
k=0
d=1
	while IFS=',' read -ra line
	do
		if [ $k -lt 9 ] 
		then
			let "k = $k + 1"
			continue
		fi
    if [ "${line[0]}" == "$keyword" ]
		then
			break
		fi
		if [ $d -lt 10 ]
			then
			echo "`basename $filename .csv`,0$d,${line[1]},${line[7]}" 
		else
			echo "`basename $filename .csv`,$d,${line[1]},${line[7]}" 
		fi
		let "k = $k + 1"
		let "d = $d + 1"
	done < "$filename"
done


#./parser.sh csv/* > weather
