
import math

class CalcMean:
   def __init__(self):
      self.mean = 0
      self.sigma = 0
      self.n = 0
      self.sum = 0
      self.sum2 = 0

   def add_value(self,value):
      self.n += 1
      self.sum += value
      self.sum2 += value*value
      self.mean = 0
      self.sigma = 0

   def calc_mean(self):
      if self.mean != 0:
         return self.mean
      if self.n == 0:
         return 0

      self.mean = float(self.sum)/float(self.n)
      return self.mean

   def calc_sigma(self):
      if self.sigma != 0:
         return self.sigma
      if self.n == 0:
         return 0
      mean = self.calc_mean()
      self.sigma = math.sqrt( (1./self.n)*self.sum2 - mean*mean)
      return self.sigma

   def get_string(self,format = '%f +/- %f',
                       show_percent_error=False,
                       show_percent_error_format = '%12.2f +/- %12.2f (%04.2f%%)'
                 ):
      s = ''
      if show_percent_error:
         percent_error = self.calc_sigma()/self.calc_mean()*100.
         s = show_percent_error_format % (self.calc_mean(),self.calc_sigma(),percent_error)
      else:
         s = format % (self.calc_mean(),self.calc_sigma())
      return s

