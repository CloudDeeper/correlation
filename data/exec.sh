#!/usr/bin/bash
for ((i=101; i<=105; i++));
do
  for ((j=1; j<=12;j++));
  do  
    if (( j < 10 )); then
				./parser.sh result/$i.0$j > new/$i.0$j
		else
				./parser.sh result/$i.$j > new/$i.$j
    fi  
  done
done

