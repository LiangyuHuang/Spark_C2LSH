#zID: z5241124

#name: Liangyu Huang

# Using Spark to implement C2LSH

​	The pseudocode of C2LSH is shown as following.

<img src="/Users/liangyu/Library/Application Support/typora-user-images/image-20200716212211936.png">

​	And the diagram of this code shows as follwing.

![diagram.001](/Users/liangyu/Documents/GitHub/Spark_C2LSH/diagram.001.jpeg)

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

Average running time is approximately 4.30s.

![image-20200716232518044](/Users/liangyu/Library/Application Support/typora-user-images/image-20200716232518044.png)



![image-20200716232416600](/Users/liangyu/Library/Application Support/typora-user-images/image-20200716232416600.png)

This is the longest time.

## Improve the efficiency

1. Improving *match* function. If the result is False, stop immediately.
2. Employing *flatmap* in C2LSH function to replace map and filter. (ie. change two action *map filter* to *flatmap* )







