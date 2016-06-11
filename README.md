HOW TO
-----------------
1. gen `weather` by `weather/weather_parser.sh csv/* > weather`
2. put weather to hdfs
3. run spark
4. gen the file `data/res` which you want to delete from original proc data
5. execute `data/exec.sh`
