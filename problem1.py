from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col
import os
import glob
import shutil
import sys

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("Problem1_LogLevelDistribution_Cluster")
    .getOrCreate()
)

# Handle net-id argument
net_id = sys.argv[-1]

# Detect mode based on master URL
if len(sys.argv) > 1 and sys.argv[1].startswith("spark://"):
    input_path = "file:/home/ubuntu/spark-cluster/data/raw/"
else:
    input_path = "data/raw/"

# Define output directory
output_dir = "data/output"
os.makedirs(output_dir, exist_ok=True)

# Read all log files
log_files = []
for root, _, files in os.walk(input_path.replace("file:", "")):
    for f in files:
        if f.endswith(".log"):
            log_files.append(os.path.join(root, f))

if not log_files:
    raise FileNotFoundError("No .log files found in data/raw/")

logs_df = spark.read.text(log_files)


# Extract timestamp, log level, and message
parsed_df = logs_df.select(
    regexp_extract('value', r'^(?:\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 0).alias('timestamp'),
    regexp_extract('value', r'\b(INFO|WARN|ERROR|DEBUG)\b', 1).alias('log_level'),
    col('value').alias('log_entry')
)

# Keep only valid log lines
valid_logs_df = parsed_df.filter(col('log_level') != "")

# Count entries per log level
counts_df = valid_logs_df.groupBy('log_level').count().orderBy('count', ascending=False)
counts_path = os.path.join(output_dir, "problem1_counts_tmp")
counts_df.coalesce(1).write.mode("overwrite").option("header", True).csv(counts_path)

# Move part file to final name
part_file = glob.glob(os.path.join(counts_path, "part-*.csv"))[0]
shutil.move(part_file, os.path.join(output_dir, "problem1_counts.csv"))
shutil.rmtree(counts_path)

# Collect a random sample of 10 entries
sample_logs = valid_logs_df.sample(False, 0.001).limit(10).toPandas()
sample_path = os.path.join(output_dir, "problem1_sample.csv")
sample_logs[['log_entry', 'log_level']].to_csv(sample_path, index=False)

# Compute summary statistics
total_lines = logs_df.count()
valid_lines = valid_logs_df.count()
unique_levels = counts_df.count()
counts = counts_df.collect()

# Write summary file
summary_lines = []
summary_lines.append(f"Total log lines processed: {total_lines:,}")
summary_lines.append(f"Total lines with log levels: {valid_lines:,}")
summary_lines.append(f"Unique log levels found: {unique_levels:,}")
summary_lines.append("\nLog level distribution:")

for row in counts:
    level = row['log_level']
    count = row['count']
    percent = (count / valid_lines) * 100 if valid_lines > 0 else 0
    summary_lines.append(f"  {level:<6}: {count:>8,} ({percent:6.2f}%)")

summary_path = os.path.join(output_dir, "problem1_summary.txt")
with open(summary_path, "w") as f:
    f.write("\n".join(summary_lines))

spark.stop()
