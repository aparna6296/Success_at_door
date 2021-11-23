source=$1
target=$2
name="GA_Insights"

spark-submit	\
--master yarn	\
--deploy-mode cluster	\
--driver-memory 2g	\
--executor-memory 5g	\
--executor-cores 2    	\
--conf "spark.dynamicAllocation.enabled=true" \
--conf "spark.sql.shuffle.partitions=1" \
--py-files jobs.zip	\
main.py --job insights --source_location $source --target_location $target --app_name $name