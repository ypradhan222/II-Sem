from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from collections import deque

class Bucket: #stores number of 1's as size and righmost 1 as its timestamp
   def __init__(self,size,timestamp):
      self.size = size
      self.timestamp = timestamp
def updation(dgim,timestamp,window_size):
   while dgim and timestamp - dgim[0].timestamp>=window_size:
      dgim = dgim[1:]   #oldest one is removed
   return dgim
def estimate(dgim,window_size):
   count =0
   for i,bucket in enumerate(dgim):
      if bucket.timestamp>=window_size:
         count+=2**i
   return count
def update_window(dgim,window_size,bit):
   if bit == 1:
      dgim.append(Bucket(1,window_size))
   dgim = updation(dgim,window_size,window_size)
   return dgim

def stream(rdd):
   global dgim  #so that changes can be seen in outside the function
   data = rdd.collect()
   for item in data:
      timestamp = item[0]
      bit = int(item[1])
      dgim = update_window(dgim,timestamp,bit)
      count = estimate(dgim,window_size)
      print(f"Estimated count at {timestamp}: {count}")
def main():
   global window_size,dgim
   window_size = 32
   dgim = []
   context = SparkContext("local[]","DGIM")
   streamcontext = StreamingContext(context,1)
   kafka_parameteres = {"metadata.broker.list":"localhost:9092"}
   directstream = KafkaUtils.createDirectStream(streamcontext,["dgim_bits"],kafka_parameteres)
   directstream.foreachRDD(stream)
   streamcontext.start()
   streamcontext.awaitTermination()

if __name__ == "__main__":
   main()

