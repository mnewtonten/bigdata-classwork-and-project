Michael Newton

# Group Questions

1. 4541 stations in Texas. Found by filtering on "TX" substring in list of stations and counting the resulting RDD.
2. 2302 stations in Texas have reported. Found by using a map to isolate the station IDs in the previous list, and the IDs in the 2017 file (distinct method used to remove duplicates). Then an intersection between the two returned an RDD containing IDs that existed in both.
3. Max temp is 64.9 degrees C in 05-22-2017 at the station ID USR0000APED. Made a case class called SData for 2017 reports and did a fold that returned an SData with the largest value for a "TMAX" elem. 
4. 66141 - list of all station IDs from 2017 that reported subtracted(method) from list of all station IDs.

# Lone Questions

1. Max rainfall in Texas is US1TXLR0014 with 6350 precipitation on 08-27-2017. I did this by filtering sData to only precipitation values from texas (based on seeing TX in ID) and then folding for max as done before. I tried so hard to implement the other RDD directly but all of my operations were timing out. I know there are some texas station IDs without TX in them but I could not include them in this answer because I could not figure out a way to get a list of all of the precipitation values in texas based on the TX state identifier in stations. I tried narrowing them down as much as I could and the only way I could think to do it was to do a fold within a fold but this would always time out.
2. Max rainfall in India is IN017111200, with 4361 precipitation on 03-09-2017. See above.
3. 28 Stations in San Antonio. Filtering TX stations based on Name value
4. 3 Stations in San Antonio have reported Temp

