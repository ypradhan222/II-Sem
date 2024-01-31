from pyspark.sql import SparkSession
import random as rd
def word_countmapper(word):
    return(word,1)
def wordcount_reducer(a,b):
    return(a+b)
def splitter(line):
    words =line.split(" ")
    return(word for word in words if word not in stop_word)

stop_word =["This","the","is","a","it","us","Sapiens"]
logFile = "wordcount.txt"
spark = SparkSession.builder.appName("WordCount").master("local").getOrCreate()
spark.sparkContext.setLogLevel("off")
read_file = spark.sparkContext.textFile(logFile)
words = read_file.flatMap(splitter)
word_count_mapped = words.map(word_countmapper)
wordcount_reduced = word_count_mapped.reduceByKey(wordcount_reducer)
temp = wordcount_reduced.collect()
for x in temp:
    print(x)
spark.stop()