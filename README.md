# BroadcastJoinWithSecondarySorting
Application build temperature reports based on IOT data files, it support two mods:
 - process;
 - reprocess.

 In 'process' mode it will generate two reports 15 minutes report and 1 hour report based on input data.
 In 'reprocess' mode application will 1 hour report based on 15 minutes report,
 path to which should be specified as input argument for the job.

 Usage: TestSparkTask [process|reprocess] [options]

 Command: process [options]
 Process 1 hour and 15 min reports
   -D, --sensor-devices-data-file <sensor-devices-data-file>
                            Absolute path to sensor device data file.

   -d, --sensor-data-file <sensor-data-file>
                            Absolute path to sensor data file.

 Command: reprocess [options]
 Reprocess 15 min report and produce 1 hour report file
   -r, --sensor-report-file <sensor-report-file>
                            Absolute path 15 minutes report file.

   -t, --target-directory <target-directory>
                            Absolute path to directory where to save reports.




   -n, --num-of-rooms <num-of-rooms>
                            Number of rooms to track.

   -f, --from-date <from-date>
                            Define lower date boundary for report generation.
 Date pattern: yyyy-MM-dd

   -u, --until-date <until-date>
                            Define upper date boundary for report generation.
 Date pattern: yyyy-MM-dd

##### Usage Spark 2
```bash
spark-submit --class com.spark.task.ApplicationReportReprocessing \
--master yarn \
--deploy-mode client \
--executor-memory 512M \
--driver-memory 512M \
--num-executors 1 \
test-task-spark-0.0.1-SNAPSHOT.jar process \
--sensor-devices-data-file /tmp/test/ds1.csv \
--sensor-data-file /tmp/test/ds2.csv \
--target-directory /tmp/test \
--num-of-rooms 4