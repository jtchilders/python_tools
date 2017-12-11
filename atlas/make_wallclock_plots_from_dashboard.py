
import json

dat = json.load(open('consumptionsjson.json'))

out = {}

dates = []
queues = []
sites = ['OLCF','ALCF','NERSC']

for entry in dat['jobs']:
   if entry['S_DATE'] in out:
      out[entry['S_DATE']][entry['S_SITE']] = entry['SUM']
   else:
      out[entry['S_DATE']] = {entry['S_SITE']:entry['SUM']}
   
   dates.append(entry['S_DATE'])
   queues.append(entry['S_SITE'])

# remove duplicate dates
dates = sorted(list(set(dates)))
queues = sorted(list(set(queues)))

outfile = open('wallclock_per_queue.csv','w')
header = 'date,%s\n' % ','.join(x for x in queues) 
outfile.write(header)
for date in dates:
   entry = out[date]
   for queue in queues:
      if queue not in entry:
         entry[queue] = 0
   line = date + ',' + ','.join(str(entry[x]/60/60) for x in queues) + '\n'
   outfile.write(line)


outfile = open('wallclock_per_site.csv','w')
header = 'date,%s\n' % ','.join(x for x in sites) 
outfile.write(header)
for date in dates:
   
   entry = out[date]
   
   new_sums = {}
   for site in sites:
      new_sums[site] = 0
   
   for site,sum in entry.iteritems():
      if 'NERSC' in site:
         new_sums['NERSC'] += sum
      elif 'ORNL' in site or 'Titan' in site:
         new_sums['OLCF'] += sum
      elif 'ALCF' in site or 'ARGO' in site:
         new_sums['ALCF'] += sum
   line = date + ',' + ','.join(str(new_sums[x]/60/60) for x in sites) + '\n'
   outfile.write(line)




