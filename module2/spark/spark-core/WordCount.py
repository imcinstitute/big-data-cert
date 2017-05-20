from operator import add
file = sc.textFile("hdfs:///user/cloudera/input/pg2600.txt")
wc = file.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
wc.saveAsTextFile("hdfs:///user/cloudera/output/wordcountPython")
