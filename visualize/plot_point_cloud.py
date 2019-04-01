import glob
import numpy as np
import pandas as pd
import pptk

fl = glob.glob('/Users/jchilders/workdir/ALCFData/electron_point_cloud_csv/*')

print('found %s files' % len(fl))

col_names = ['id', 'index', 'x', 'y', 'z', 'eta', 'phi','r','Et','pid','true_pt']
col_dtype = {'id': np.int64, 'index': np.int64, 'x': np.float32, 'y': np.float32,
             'z': np.float32, 'eta': np.float32, 'phi': np.float32, 'r': np.float32,
             'Et': np.float32, 'pid': np.int32, 'true_pt': np.float32}

for filename in fl:
   points = pd.read_csv(open(filename), names=col_names, dtype=col_dtype, sep='\t')


   v = pptk.viewer(points[['x','y','z']])
   v.set(point_size=5)
   attr = np.int32(points[['Et']])
   attr = np.reshape(attr,(attr.shape[0],))
   v.attributes(attr)
   v.set(show_grid=False)

   print('here')
   v.wait()

