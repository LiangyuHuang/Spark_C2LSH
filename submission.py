## import modules here

########## Question 1 ##########
# do not change the heading of the function


def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = -1
    numCandidates = -1
    data_hashes = data_hashes.map(lambda x: (x[0], differ(x, query_hashes)))
    while numCandidates < beta_n:
        offset += 1
        result = data_hashes.flatMap(lambda x: [x[0]] if match(x, alpha_m, offset) else [])
        numCandidates = result.count()
        # print("offset is:", offset)
        # print("candidate number is:", numCandidates)
    return result


def differ(data_hash, query_hash):
    list_deal = data_hash[1]
    list_different = []
    for i in range(len(list_deal)):
        list_different.append(abs(list_deal[i] - query_hash[i]))
    return list_different


def match(hash_list, alpha_m, offset):
    target = hash_list[1]
    count = 0
    for i in range(0, len(target)):
        if target[i] <= offset:
            count += 1
        if count >= alpha_m:
            return True
    return False
