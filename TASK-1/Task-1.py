##TASK 1
import pandas as pd

# No. 1
pd.set_option('display.max_column', None)

df = pd.read_csv("dataset/sample.csv", sep=',')
# print(df.head(1))

# No. 2
re_col = df.rename(columns={
    'VendorID' : 'vendor_id',
    'RatecodeID' : 'rate_code_id',
    'PULocationID': 'pu_location_id',
    'DOLocationID' : 'do_location_id',
    })
# print(re_col.dtypes)

# No. 3
top_10_passenger = df.nlargest(10, 'passenger_count') [['VendorID', 'passenger_count', 'trip_distance', 'payment_type', 
'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge']]
# print(top_10_passenger)


# No. 4
df['passenger_count'].fillna(0, inplace=True)
df['passenger_count'] = df['passenger_count'].astype(int)

df['tpep_pickup_datetime'].fillna(0, inplace=True)
df['tpep_pickup_datetime']= df['tpep_pickup_datetime'].astype('datetime64[ns]')

df['tpep_dropoff_datetime'].fillna(0, inplace=True)
df['tpep_dropoff_datetime']= df['tpep_dropoff_datetime'].astype('datetime64[ns]')

df['tolls_amount'].fillna(0, inplace=True)
df['tolls_amount']= df['tolls_amount'].astype(float)
print(df.dtypes)