#zID: z5241124

#name: Liangyu Huang

# Using Spark to implement C2LSH

​	The pseudocode of C2LSH is shown as following.

​	And the diagram of this code shows as follwing.

Function *c2lsh*(data_hashes, query_hashes, alpha_m, beta_n):

1. Initializing the values

	2. Sending RDD to other functions

Function *differ*:

1. Initializing a list.

	2. Cauculating the differences between  data_hashes[1] and query_hashes and append the differences into the list.
 	3. Return hash with hash[0]= data_hashes[1], hash[1]=list of differences

Function *match*:

1. Compare the hash[1]\(list of differences) with offset

2. If the number of elements in hash[1] which is less than or equal to offset is equal to or greater than alpha_m, return True. Else, return False

## output

Here is the output for 1 million numbers:

Average running time is approximately 4.30s

## Improve the efficiency

1. Improving *match* function. If the result is False, stop immediately.
2. Employing *flatmap* in C2LSH function to replace map and filter. (ie. change two action *map filter* to *flatmap* )







