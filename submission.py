## import modules here

########## Question 1 ##########
# do not change the heading of the function
def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = -1
    numCandidates = -1
    while numCandidates < beta_n:
        offset += 1
        candidatesRDD = data_hashes.flatMap(lambda e: [e[0]] if match(e[1], query_hashes, alpha_m, offset) else [])
        numCandidates = candidatesRDD.count()
    return candidatesRDD


def match(dataHashCode, queryHashCode, alpha_m, offset):
    count = 0
    for i in range(0, len(dataHashCode)):
        if abs(dataHashCode[i] - queryHashCode[i]) <= offset:
            count += 1
    return count >= alpha_m
