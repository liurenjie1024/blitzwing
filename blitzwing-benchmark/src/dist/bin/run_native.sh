#!/usr/bin/env bash
SPARK_HOME="/root/liu/spark/spark-3.0.0-SNAPSHOT-bin-spark3-native"
APPLICATION_HOME="/root/liu/spark/scripts/benchmark-0.0.1-SNAPSHOT"
# --driver-java-options "-XX:+PreserveFramePointer -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -agentpath:/root/liu/spark/profiler/build/libasyncProfiler.so=start,file=profile_native.svg" \

	#--driver-library-path /root/liu/spark/arrow-executor2/arrow-executor-rust/target/release \
$SPARK_HOME/bin/spark-submit --master local[8] \
	--class com.ebay.hadoop.spark.BenchMark \
	--jars $APPLICATION_HOME/lib/scopt_2.12-3.7.1.jar \
	--conf spark.sql.arrow.native.execution.enabled=true \
	--conf spark.sql.adaptive.enabled=false \
	$APPLICATION_HOME/lib/benchmark-0.0.1-SNAPSHOT.jar \
	-p file:///root/liu/spark/data/10m/*.parquet \
	-s "select sum(vstr_yn_id), sum(flags01), sum(session_skey), sum(seqnum), sum(cobrand), sum(source_impr_id), sum(agent_id), sum(watch_page_id), sum(soj_watch_flags64), sum(has_dw_rec), sum(bot_ind), sum(del_session_skey), sum(del_seqnum), sum(del_cobrand), sum(del_agent_id), sum(del_page_id), sum(del_flags64), sum(del_bot_ind), sum(format_flags64) from t"

