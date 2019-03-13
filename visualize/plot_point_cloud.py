import csv
import numpy as np
import pandas as pd
import pptk

filename = '/projects/atlasMLbjets/parton/csv_data/electrons/100GEV_0ETA_0PHI/794ec883-57e7-4e43-9845-37648c60d9ac_nevts1_evtid00000005_graphcnn.csv'

col_names = ['id', 'index', 'x', 'y', 'z', 'eta', 'phi','r','Et','pid','true_pt']
col_dtype = {'id': np.int64, 'index': np.int64, 'x': np.float32, 'y': np.float32,
             'z': np.float32, 'eta': np.float32, 'phi': np.float32, 'r': np.float32,
             'Et': np.float32, 'pid': np.int32, 'true_pt': np.float32}
points = pd.read_csv(open(filename), names=col_names, dtype=col_dtype, sep='\t')


v = pptk.viewer(points[['x','y','z']])
v.set(point_size=5)
attr = np.int32(points[['Et']])
attr = np.reshape(attr,(attr.shape[0],))
v.attributes(attr)
v.set(show_grid=False)

print('here')

