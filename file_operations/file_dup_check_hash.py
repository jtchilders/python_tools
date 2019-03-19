#!/usr/bin/env python
import argparse,logging,glob,hashlib,random,json
logger = logging.getLogger(__name__)


def main():
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging_format = '%(asctime)s %(levelname)s:%(name)s:%(message)s'
   logging_datefmt = '%Y-%m-%d %H:%M:%S'
   logging_level = logging.INFO
   
   parser = argparse.ArgumentParser(description='')
   parser.add_argument('-g','--glob',dest='glob',help='input glob string to search for files',required=True)
   parser.add_argument('-n','--nfiles',dest='nfiles',default=-1,type=int,help='input glob string to search for files')
   parser.add_argument('-o','--outjson',dest='outjson',default='output.json',help='hash table will be stored to this file')
   parser.add_argument('-i','--injson',dest='injson',help='if given, will be used for hash table instead of creating one.')

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
     

   filelist = sorted(glob.glob(args.glob))
   fileindex = range(len(filelist))
   #hashlist = ['' for i in range(len(filelist))]
   logger.info('found %s files',len(filelist))
   nfiles = len(filelist)
   if args.nfiles > 0:
      nfiles = args.nfiles


   logger.info('hashing all files')
   hashlist = []
   for i in range(len(filelist)):
      if i % 100 == 99:
         logger.info(' %5d of %5d ',i,len(filelist))
      buf = open(filelist[i],'r').read()
      hashlist.append(hashlib.md5(buf).hexdigest())
  
   json.dump(hashlist,open(args.outjson,'w'))


   
   logger.info('verifying %s files',nfiles)
   for _ in range(nfiles):
      
      
      checki = random.choice(fileindex)
      dupes = 0
      for j in range(len(filelist)):
         if checki == j: continue
         if hashlist[checki] == hashlist[j]:
            logger.info('files %s and %s match',filelist[checki],filelist[j])
            dupes += 1


      logger.info('found %s duplicates of file %s',dupes,filelist[checki])
      


if __name__ == "__main__":
   main()
