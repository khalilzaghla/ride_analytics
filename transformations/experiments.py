from pyspark.sql import functions as F

df = spark.read.table("workspace.realtime.silver_ride_events")

# 1. Schema
df.printSchema()

# 2. Row count + status breakdown
df.groupBy("status").count().show()

# 3. What does event_time actually look like?
df.select("event_time", "status", "final_fare").show(5, truncate=False)

# 4. Min/max event_time in the table
df.agg(F.min("event_time"), F.max("event_time")).show(truncate=False)

# 5. What is current_timestamp() on this cluster?
spark.sql("SELECT current_timestamp(), current_timezone()").show(truncate=False)