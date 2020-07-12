## import modules here

########## Question 1 ##########
# do not change the heading of the function
def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = -1
    numCandidate = -1
    while numCandidate < beta_n:
        offset += 1
        candidatesRDD = data_hashes.filter(lambda e: match(e[2],query_hashes,alpha_m,offset )).map( lambda e: e[0])
    return candidatesRDD

def match(dataHashCode, queryHashCode, alpha_m, offset):
    count = 0
    for i in range(0, len(dataHashCode)):
        if abs(dataHashCode - queryHashCode) <= offset:
            count += 1
    return count >= alpha_m