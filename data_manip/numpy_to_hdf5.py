#!/usr/bin/env python
import argparse,logging,glob
import numpy as np
from mpi4py import MPI
import h5py
logger = logging.getLogger(__name__)


def main():
   rank = MPI.COMM_WORLD.Get_rank()
   nranks = MPI.COMM_WORLD.Get_size()
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging_format = '%(asctime)s %(levelname)s:' + ('%05d' % rank) + ':%(name)s:%(message)s'
   logging_datefmt = '%Y-%m-%d %H:%M:%S'
   logging.basicConfig(level=logging.INFO,format=logging_format,datefmt=logging_datefmt)

   parser = argparse.ArgumentParser(description='')
   parser.add_argument('-g','--glob',help='glob pattern to get input files',required=True)
   parser.add_argument('--inputs_per_file',help='number of images per input file',default=1,type=int)
   parser.add_argument('-n','--outputs_per_file',help='number of images per output file',type=int,required=True)
   parser.add_argument('-o','--output_filebase',help='base output file name, can include path',default='output')
   parser.add_argument('--max_particles',help='maximum truth particles per image',default=5,type=int)
   parser.add_argument('-z','--gzip',help='enable libz compression on outputs',default=False,action='store_true')

   parser.add_argument('--debug', dest='debug', default=False, action='store_true', help="Set Logger to DEBUG")
   parser.add_argument('--error', dest='error', default=False, action='store_true', help="Set Logger to ERROR")
   parser.add_argument('--warning', dest='warning', default=False, action='store_true', help="Set Logger to ERROR")
   parser.add_argument('--logfilename',dest='logfilename',default=None,help='if set, logging information will go to file')
   args = parser.parse_args()


   if args.debug and not args.error and not args.warning:
      # remove existing root handlers and reconfigure with DEBUG
      for h in logging.root.handlers:
         logging.root.removeHandler(h)
      logging.basicConfig(level=logging.DEBUG,
                          format=logging_format,
                          datefmt=logging_datefmt,
                          filename=args.logfilename)
      logger.setLevel(logging.DEBUG)
   elif not args.debug and args.error and not args.warning:
      # remove existing root handlers and reconfigure with ERROR
      for h in logging.root.handlers:
         logging.root.removeHandler(h)
      logging.basicConfig(level=logging.ERROR,
                          format=logging_format,
                          datefmt=logging_datefmt,
                          filename=args.logfilename)
      logger.setLevel(logging.ERROR)
   elif not args.debug and not args.error and args.warning:
      # remove existing root handlers and reconfigure with WARNING
      for h in logging.root.handlers:
         logging.root.removeHandler(h)
      logging.basicConfig(level=logging.WARNING,
                          format=logging_format,
                          datefmt=logging_datefmt,
                          filename=args.logfilename)
      logger.setLevel(logging.WARNING)
   else:
      # set to default of INFO
      for h in logging.root.handlers:
         logging.root.removeHandler(h)
      level = logging.INFO
      if rank != 0:
         level = logging.WARNING
      logging.basicConfig(level=level,
                          format=logging_format,
                          datefmt=logging_datefmt,
                          filename=args.logfilename)

   logger.info('rank                      %s of %s',rank,nranks)
   logger.info('glob string:              %s',args.glob)
   logger.info('inputs_per_file:          %s',args.inputs_per_file)
   logger.info('outputs_per_file:         %s',args.outputs_per_file)
   logger.info('output_filebase:          %s',args.output_filebase)
   logger.info('max_particles:            %s',args.max_particles)

   filelist = sorted(glob.glob(args.glob))
   logger.info('found files:              %s',len(filelist))

   total_inputs = len(filelist)*args.inputs_per_file
   logger.info('total inputs to process:  %s',total_inputs)

   total_output_files = int( total_inputs / args.outputs_per_file )
   logger.info('total output files:       %s',total_output_files)


   outputs_per_input = int(args.outputs_per_file/args.inputs_per_file)

   
   for i in range(int(total_output_files/nranks)+1):

      output_file_number = nranks*i + rank
      logger.info('output_file_number:       %s  of  %s',output_file_number,total_output_files)

      if output_file_number >= total_output_files:
         continue

      input_start_file_index = rank*outputs_per_input
      input_end_file_index   = (rank+1)*outputs_per_input

      output_data = None
      output_truth = None
      image_counter = 0
      for file_index in range(input_start_file_index,input_end_file_index):
         try:
            filename = filelist[file_index]
            logger.debug('opening filename: %s',filename)
            npfile = np.load(filename)
            data = npfile['raw']
            truth = npfile['truth']

            if output_data is None:
               output_data = np.zeros(shape=(args.outputs_per_file,) + data.shape[1:])
               output_truth = np.zeros(shape=(args.outputs_per_file,args.max_particles) + truth.shape[2:])

            output_data[image_counter,...] = data[0,...]
            if truth.shape[1] > args.max_particles:
               logger.error('truth particles shape: %s ',truth.shape)
            output_truth[image_counter,0:truth.shape[1],...] = truth[0,...]

            image_counter += 1
         except:
            logger.exception('exception received while processing file %s',filename)

      hdfilename = '%s_%05d.h5' % (args.output_filebase,output_file_number)
      logger.info('writing file: %s',hdfilename)
      hdfile = h5py.File(hdfilename,'w')
      compression = None
      if args.gzip:
         compression = 'gzip'
      hdfile.create_dataset('raw',data=output_data,compression=compression)
      hdfile.create_dataset('truth',data=output_truth,compression=compression)
      hdfile.close()


def modify_raw(raw):
   batch_size,channels,height,width = raw.shape
   new_raw = np.zeros([batch_size,height,width,channels])



   return 




   
   


if __name__ == "__main__":
   main()
