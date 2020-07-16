from pyspark import SparkConf
from pyspark import SparkContext
import submission
import pickle
import time


def createSC():
    conf = SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("C2LSH")
    sc = SparkContext(conf=conf)
    return sc


with open("./testCases/testCase1.pkl", "rb") as file:
    data = pickle.load(file)

with open("./testCases/testQueryHash.pkl", "rb") as file:
    query_hashes = pickle.load(file)

alpha_m = 10
beta_n = 100000

sc = createSC()
data_hashes = sc.parallelize([(index, x) for index, x in enumerate(data)])
start_time = time.time()
res = submission.c2lsh(data_hashes, query_hashes, alpha_m, beta_n).collect()
end_time = time.time()
sc.stop()

print('running time:', end_time - start_time)
print('Number of candidate: ', len(res))
#print('set of candidate: ', set(res))
