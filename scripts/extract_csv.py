#!/bin/python2

import sys, os
import pandas as pd
from sets import Set


stop_data = pd.read_csv(sys.argv[1], sep = ",", dtype=str)
route_data = pd.read_csv(sys.argv[2], sep = ",", dtype=str)
sequence_data = pd.read_csv(sys.argv[3], sep = ",", dtype=str)
trip_data = pd.read_csv(sys.argv[4], sep = ",", dtype=str)

## Extracting Stops per rout

next_stop_id = 0
next_route_id = 0
new_route_ids = {}
new_stop_ids = {}
trip_to_route = {}
def compute_trip_route(row):
    global next_route_id
    global trip_to_route
    route_id = str(row["route_id"])
    trip_id = str(row["trip_id"])
    trip_to_route[trip_id] = route_id
    if route_id not in new_route_ids:
        new_route_ids[route_id] = next_route_id
        next_route_id = next_route_id + 1

trip_data.apply(compute_trip_route, axis = 1)

stop_to_route = {};
def compute_stop_route(row):
    global stop_to_route
    global next_stop_id
    stop_id = str(row["stop_id"])
    trip_id = str(row["trip_id"])
    route_id = trip_to_route[trip_id]
    if stop_id not in stop_to_route:
        stop_to_route[stop_id] = Set()
    stop_to_route[stop_id].add(route_id)

    if stop_id not in new_stop_ids:
        new_stop_ids[stop_id] = next_stop_id
        next_stop_id = next_stop_id + 1

sequence_data.apply(compute_stop_route, axis = 1)


## Extracting Stops Graph
sequence_data["previous_stop_sequence"] = sequence_data["stop_sequence"].shift(1)
sequence_data["previous_stop_id"] = sequence_data["stop_id"].shift(1)

def compute_stop_edges(row):
    if row["previous_stop_sequence"] < row["stop_sequence"]:
        edge =  str(row["previous_stop_id"])+"|"+str(row["stop_id"])
        return edge
    else:
        return "noedge"

sequence_data["edges"] = sequence_data.apply(compute_stop_edges, axis = 1)

edges = Set();
def filter_edges(edge):
    if edge != "noedge":
        edges.add(edge)

sequence_data["edges"].apply(filter_edges)

stop_to_stop_file = open("stop_edges.csv",'w')
stop_to_stop_file.write("stop.id,stop.id\n")
for edge in edges:
    pair = edge.split("|")
    if pair[0] != "nan" and pair[1] != "nan":
        stop_to_stop_file.write(str(new_stop_ids[pair[0]])+","+str(new_stop_ids[pair[1]])+"\n")
stop_to_stop_file.close()

stop_to_route_file = open("stop_to_route.csv",'w')
stop_to_route_file.write("stop.id,route.id\n")
for stop_id in stop_to_route:
    for route_id in stop_to_route[stop_id]:
        stop_to_route_file.write(str(new_stop_ids[stop_id])+","+str(new_route_ids[route_id])+"\n");
stop_to_route_file.close();

stop_data["new_ids"] = stop_data["stop_id"].apply( lambda x : new_stop_ids[str(x)])
stop_data[["new_ids","stop_name","stop_lat","stop_lon"]].to_csv("stops.csv",sep=",",header=["stop.id","stop.name","stop.lat","stop.lon"],index = False)
route_data["new_ids"] = route_data["route_id"].apply( lambda x : new_route_ids[str(x)])
route_data[["new_ids","route_short_name","route_long_name","route_type"]].to_csv("routes.csv",sep=",",header=["route.id","route.short_name","route.long_name","route.type"], index = False)

