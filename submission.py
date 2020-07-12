## import modules here

########## Question 1 ##########
# do not change the heading of the function
def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = -1
    numCandidates = -1

    def differ(data_hash, query_hash):
        list_different = [abs(data_hash[i] - query_hash[i]) for i in range(0, len(data_hash))]
        return list_different

    data_hashes = data_hashes.map(lambda e: (e[0], differ(e[1], query_hashes)))
    while numCandidates < beta_n:
        offset += 1
        candidatesRDD = data_hashes.flatMap(lambda e: [e[0]] if match(e[1], alpha_m, offset) else [])
        numCandidates = candidatesRDD.count()
    return candidatesRDD


def match(list_differ, alpha_m, offset):
    count = 0
    for i in range(0, len(list_differ)):
        if list_differ[i] <= offset:
            count += 1
        if count >= alpha_m:
            return True
    return False
