#!/usr/bin/env python
import argparse,logging
logger = logging.getLogger(__name__)
import glob
import numpy as np
import pandas as pd
import pptk

point_values = ['Et','trk_id','trk_notrk','pid']


def main():
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging_format = '%(asctime)s %(levelname)s:%(name)s:%(message)s'
   logging_datefmt = '%Y-%m-%d %H:%M:%S'
   logging_level = logging.INFO
   
   parser = argparse.ArgumentParser(description='')
   parser.add_argument('-g','--glob',help='input glob for files',default='/Users/jchilders/workdir/ml_data/atlas/csv_data/100GEV_0ETA_0PHI_v2/bjets/*')
   parser.add_argument('-v','--value',help='what value to use for point colors, can be %s' % str(point_values),default='Et')
   parser.add_argument('-t','--trkonly',help='only plot points from tracker',default=False,action='store_true')
   parser.add_argument('--debug', dest='debug', default=False, action='store_true', help="Set Logger to DEBUG")
   parser.add_argument('--error', dest='error', default=False, action='store_true', help="Set Logger to ERROR")
   parser.add_argument('--warning', dest='warning', default=False, action='store_true', help="Set Logger to ERROR")
   parser.add_argument('--logfilename',dest='logfilename',default=None,help='if set, logging information will go to file')
   args = parser.parse_args()

   if args.debug and not args.error and not args.warning:
      logging_level = logging.DEBUG
   elif not args.debug and args.error and not args.warning:
      logging_level = logging.ERROR
   elif not args.debug and not args.error and args.warning:
      logging_level = logging.WARNING

   logging.basicConfig(level=logging_level,
                       format=logging_format,
                       datefmt=logging_datefmt,
                       filename=args.logfilename)
   
   if args.value not in point_values:
      logger.error('point value selection not in allowed options %s',point_values)
      raise Exception('invalid value option')

   logger.info('glob string: %s',args.glob)
   fl = glob.glob(args.glob)

   logger.info('found %s files',len(fl))

   col_names = ['id', 'index', 'x', 'y', 'z', 'eta', 'phi', 'r', 'Et','pid','n','trk_good','trk_id','trk_pt']
   col_dtype = {'id': np.int64, 'index': np.int32, 'x': np.float32, 'y': np.float32,
                'z': np.float32, 'eta': np.float32, 'phi': np.float32, 'r': np.float32,
                'Et': np.float32, 'pid': np.float32, 'n': np.float32, 'trk_good': np.float32, 'trk_id': np.float32, 'trk_pt': np.float32}
   
   for filename in fl:
      logger.info('filename: %s',filename)
      points = pd.read_csv(open(filename), names=col_names, dtype=col_dtype, sep='\t')

      logger.info('pid=%s' % points['pid'].unique())

      #print(points[['x','y','z']].as_matrix())
      logger.info('len(points) = %s',len(points))
      if args.trkonly:
         points = points[points['id'] < 2e18]  # tracker only
         logger.info('len(points) = %s',len(points))

      if 'trk_notrk' in args.value:
         points_trk_id = points['trk_id'] > 0
         logger.info('points_trk_id.values = %s', points_trk_id.values)
         
         attr = np.int32(points_trk_id)
         attr = np.reshape(attr,(attr.shape[0],))
      elif 'Et' in args.value:
         points_et = points['Et']
         attr = np.float32(points_et)
         attr = np.reshape(attr,(attr.shape[0],))
      elif 'pid' in args.value:
         points_pid = points['pid'].map({-99:0,-11:10,11:10,0:5,-13:10,13:10})
         points_pid = points_pid.fillna(-1)
         #points_pid = points['pid'] >= 0.
         attr = np.int32(points_pid)
         attr = np.reshape(attr,(attr.shape[0],))
      
      # logger.info('points = \n %s',points)
      v = pptk.viewer(points[['x','z','y']])
      v.set(point_size=5)
      v.attributes(attr)
      v.set(show_grid=True)
      v.set(show_axis=False)
      # v.set(r=1400)
      v.set(bg_color=(0,0,0,0),floor_color=(0,0,0,0)) # black background
      # v.set(bg_color=(1,1,1,1),floor_color=(1,1,1,1)) # white background
      logger.info('close window for next')
      v.wait()
      v.close()


if __name__ == "__main__":
   main()
