#!/usr/bin/env python
import argparse,logging
logger = logging.getLogger(__name__)


def main():
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging_format = '%(asctime)s %(levelname)s:%(name)s:%(message)s'
   logging_datefmt = '%Y-%m-%d %H:%M:%S'
   logging_level = logging.INFO
   
   parser = argparse.ArgumentParser(description='')
   parser.add_argument('-i','--input',help='input',required=True)
   parser.add_argument('--debug', default=False, action='store_true', help="Set Logger to DEBUG")
   parser.add_argument('--error', default=False, action='store_true', help="Set Logger to ERROR")
   parser.add_argument('--warning', default=False, action='store_true', help="Set Logger to ERROR")
   parser.add_argument('--logfilename',default=None,help='if set, logging information will go to file')
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
   
   


if __name__ == "__main__":
   main()
