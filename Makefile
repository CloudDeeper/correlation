all:
	spark-submit --class ReachabilityQuery --num-executors 30 target/scala-2.10/reachability-query-application_2.10-1.0.jar data_collection/result/ final/weather > o
