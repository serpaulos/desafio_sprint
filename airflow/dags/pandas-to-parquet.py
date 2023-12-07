import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Read JSON into DataFrame
df = pd.read_json("../air_data/data-sample_data-nyctaxi-trips-2010-json_corrigido.json", lines=True)
# print(df)

# Columns: [vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, pickup_longitude,
# pickup_latitude, rate_code, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
# fare_amount, surcharge, tip_amount, tolls_amount, total_amount]


print(f"pre: Missing passenger count {df['passenger_count'].isin([0]).sum()}")
df = df[df['passenger_count'] != 0]
print(f"post: Missing passenger count {df['passenger_count'].isin([0]).sum()}")

print(f"pre: Missing passenger count {df.isna().sum()}")
df = df.fillna(0)
print(f"post: Missing passenger count {df.isna().sum()}")

df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], format='mixed').dt.tz_convert(None)
df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'], format='mixed').dt.tz_convert(None)

print("Data Types of The Columns in Data Frame")
print(df.dtypes)

# # Convert DataFrame to Arrow Table
# table = pa.Table.from_pandas(df)
#
# # Write Arrow Table to Parquet file
# pq.write_table(table, '../air_data/output.parquet')

# df['new'] = pd.to_datetime(df['new'], format='mixed').dt.tz_convert(None)
