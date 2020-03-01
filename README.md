## Step 3 - Questions
#### 1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
Throughput and latency can be evaluated using metrics such as "numInputRows", "inputRowsPerSecond", "processedRowsPerSecond".
Performance (throughput and latency) can be affected by many variables, e.g. readstream configuration options, payload size, etc. For this answer, some configuration variables are maxRatePerPartition, maxOffsetsPerTrigger, and trigger option. In a production environment where volume is huge, having too large maxOffsetsPerTrigger for a narrow trigger interval, i.e. having a huge batch size to process in a small window of time, can affect performance negatively, i.e. processedRowsPerSecond < inputRowsPerSecond.

#### 2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?
Compare performance metrics mentioned in question 1 to arrive at these key-value pairs:

readStream:
###### spark.streaming.kafka.maxRatePerPartition = 300
######spark.streaming.backpressure.enabled: true
writeStream:
spark.sql.streaming.ProcessingTime: "60 seconds"
