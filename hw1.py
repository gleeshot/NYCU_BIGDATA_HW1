import glob
import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt

def get_merged_csv(flist, **kwargs):
    return pd.concat([pd.read_csv(f, **kwargs) for f in flist], ignore_index = True)

path = "./"
fmask = os.path.join(path, "*yellow_tripdata*.csv")

# data read and preprocess
print("# read data and preprocess it\n")
df = get_merged_csv(glob.glob(fmask), index_col = None, usecols = ["Passenger_Count", "Start_Lon", "Start_Lat", "End_Lon", "End_Lat", "Trip_Pickup_DateTime", "Total_Amt"])
print(df)
print("\n")

# delete the outlier
print("# delete outlier\n")
df__filter = df[(df.Start_Lon > -75) & (df.Start_Lon < -72) & (df.Start_Lat > 39) & (df.Start_Lat < 42) & (df.End_Lon > -75) & (df.End_Lon < -72) & (df.End_Lat > 39) & (df.End_Lat < 42)]
print(df__filter)
print("\n")

'''print("==============Q1_1===================")
print("find most pickup region and top 5 pickup regions\n")
df_round = df__filter.round({"Start_Lon" : 2, "Start_Lat" : 2})
print(df_round.dtypes)
print("\n")

# concat start_lon and start_lat as start location
df_round[["Start_Lon","Start_Lat"]] = df_round[["Start_Lon","Start_Lat"]].astype(str)
print(df_round.dtypes)
print("# create start location")
df_round["start_location"] = df_round["Start_Lon"] + df_round["Start_Lat"]
print(df_round)
print("\n")

# count the passenger count of each start location
print("# count passenger count")
df_groupby = df_round.groupby(["start_location"])["Passenger_Count"].sum().reset_index()
# sort the value by decending
df_most_pickups = df_groupby.sort_values(by = ["Passenger_Count"], ascending = False)
print(df_most_pickups)
print("\n")

# release memory space
del df_most_pickups
del df_round
del df_groupby

print("==============Q1_2===================")
print("find top 5 dropoff regions\n")
df_round_2 = df__filter.round({"End_Lon" : 2, "End_Lat" : 2})
print(df_round_2.dtypes)

# create end location
df_round_2[["End_Lon","End_Lat"]] = df_round_2[["End_Lon","End_Lat"]].astype(str)
print(df_round_2.dtypes)
print("# create end location")
df_round_2["End_location"] = df_round_2["End_Lon"] + df_round_2["End_Lat"]
print(df_round_2) # check if the col is created
print("\n")

# count the passenger count of each end location
print("# count passenger count")
df_groupby_2 = df_round_2.groupby(["End_location"])["Passenger_Count"].sum().reset_index()
df_most_dropoffs = df_groupby_2.sort_values(by = ["Passenger_Count"], ascending = False)
print(df_most_dropoffs)
print("\n")

# release memory space
del df_most_dropoffs
del df_round_2
del df_groupby_2

print("==============Q2===================")
print("find peak hour and off-peak hour\n")

df_qq = df__filter[["Passenger_Count", "Trip_Pickup_DateTime"]]
print(df_qq)
print("\n")

# get info of hour
print("# get hour info")
df_qq["Trip_Pickup_DateTime"] = pd.to_datetime(df_qq["Trip_Pickup_DateTime"])
df_qq["Hour"] = df_qq["Trip_Pickup_DateTime"].dt.hour
print(df_qq)
print("\n")

# count peak hour
print("# count peak hour")
df_qq_groupby = df_qq.groupby(["Hour"])["Passenger_Count"].sum().reset_index(name = "counts")
df_peak_hour = df_qq_groupby.sort_values(by = ["counts"], ascending = False)
print(df_peak_hour)
print("\n")

# release memory space
del df_peak_hour
del df_qq_groupby
del df_qq'''

print("==============Q3===================")
print("difference between big and small total amount\n")

# see if there is a relation between time and total amount
print("### see if there is a relation between time and total amount ###")
df_qqq_hour = df__filter[["Trip_Pickup_DateTime", "Total_Amt"]]
print(df_qqq_hour)
print("\n")

df_qqq_hour["tmp"] = df_qqq_hour["Total_Amt"]
df_qqq_hour.loc[df_qqq_hour["tmp"] < 60, "Total_Amt"] = 0
df_qqq_hour.head()
df_qqq_hour.loc[df_qqq_hour["tmp"] >= 60, "Total_Amt"] = 1

# get info of hour
print("# get hour info")
df_qqq_hour["Trip_Pickup_DateTime"] = pd.to_datetime(df_qqq_hour["Trip_Pickup_DateTime"])
df_qqq_hour["Hour"] = df_qqq_hour["Trip_Pickup_DateTime"].dt.hour
print(df_qqq_hour)
print("\n")

# count total amount
print("# count total amount")
df_qqq_hour_gb = df_qqq_hour.groupby(["Total_Amt", "Hour"]).size().reset_index(name = "hour_count")
df_qqq_hour_sort = df_qqq_hour_gb.sort_values(by = ["Total_Amt"], ascending = False)
print(df_qqq_hour_sort)
print("\n")
df_qqq_hour_sort.to_csv("time_amt.csv")

del df_qqq_hour_sort
del df_qqq_hour_gb
del df_qqq_hour

# see if there is a relation between total amount and pickup regions
print("### see if there is a relation between total amount and pickup regions ###")
df_qqq_pu = df__filter[["Start_Lon", "Start_Lat", "Total_Amt"]]
print(df_qqq_pu)
print("\n")

df_qqq_pu["tmp"] = df_qqq_pu["Total_Amt"]
df_qqq_pu.loc[df_qqq_pu["tmp"] < 60, "Total_Amt"] = 0
df_qqq_pu.loc[df_qqq_pu["tmp"] >= 60, "Total_Amt"] = 1

df_qqq_pu_rd = df_qqq_pu.round({"Start_Lon" : 2, "Start_Lat" : 2})
df_qqq_pu_rd[["Start_Lon","Start_Lat"]] = df_qqq_pu_rd[["Start_Lon","Start_Lat"]].astype(str)

# create start location
print("# create start location")
df_qqq_pu_rd["start_location"] = df_qqq_pu_rd["Start_Lon"] + df_qqq_pu_rd["Start_Lat"]
print(df_qqq_pu_rd)
print("\n")

# count total amount
print("# count total amount")
df_qqq_pu_gb = df_qqq_pu_rd.groupby(["Total_Amt", "start_location"]).size().reset_index(name = "region count")
df_qqq_pu_sort = df_qqq_pu_gb.sort_values(by = ["Total_Amt"], ascending = False)
print(df_qqq_pu_sort)
print("\n")

del df_qqq_pu_sort
del df_qqq_pu_gb
del df_qqq_pu_rd
del df_qqq_pu