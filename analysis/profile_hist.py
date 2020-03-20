import os,sys,dateutil
import pandas as pd
import numpy as np

''' example usage:
ts = get_profile_timeseries(dataframe['timestamp'],dataframe['value'])

fig,ax = plt.subplots(figsize=(6,8),dpi=160)
ax.errorbar(ts['timestamp'],ts['mean'],yerr=ts['stdev'],linestyle='None')
plt.xticks(rotation='vertical')
plt.show()
'''


def get_bin_edges_datetime(x,bins=100):
  lowest = x.min()
  highest = x.max()
  total_duration = highest - lowest
  bin_size = total_duration / bins
  bin_edges = [lowest]
  for i in range(1,bins + 1):
    bin_edges.append(bin_edges[-1] + bin_size)
  
  return bin_edges


def get_profile_timeseries(x_data,y_data,bins=100):
  bin_edges = get_bin_edges_datetime(x_data,bins)
  sum = 0.
  sum2 = 0.
  n = 0
  bin_counter = 0
  assert(len(x_data) == len(y_data))
  
  bdf = pd.DataFrame()
  bdf['timestamp'] = []
  bdf['mean'] = []
  bdf['stdev'] = []
  
  for i in range(len(x_data)):
    x = x_data[i]
    y = y_data[i]
    if x > bin_edges[bin_counter + 1]:
      # reached the end of a bin, calculate mean/stdev and add to list
      bdf = bdf.append({
        'timestamp': bin_edges[bin_counter] + ((bin_edges[bin_counter + 1] - bin_edges[bin_counter]) / 2.),
        'mean': sum / n,
        'stdev': np.sqrt((1 / n) * sum2 - (sum / n) ** 2),
      },ignore_index=True)
      sum = 0.
      sum2 = 0.
      n = 0
      bin_counter += 1
      assert bin_counter + 1 < len(bin_edges), f'{bin_counter} out of range'
    
    sum += y
    sum2 += y * y
    n += 1
  
  return bdf
