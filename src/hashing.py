#!/bin/env python3

#import random
# from sys import maxsize
# from datetime import datetime
from random import randint, choice
from math import log, ceil, floor

'''
Hashing based on Thorups 'High speed hashing for integers and strings' (2015)
The following hash function methods are for 2-universal hashing and 
used in the sketches.
'''

#using &-mask to discard overflow, i..e modulus.
#right shift to handle division by power of two (faster than div)
def hash_func(mask,b_shift,a,x):
    return ((a*x) & mask) >> b_shift

#Hashing h_a,b(j): [2^w] -> {1,-1}
def hash_func_binary(mask,b_shift,a,x):
    h = ((a*x) & mask) >> b_shift
    return h*2-1 #mask via & to get first bit only

def get_hash_function(M, N):
    w = ceil(log(M,2))
    l = ceil(log(N,2))

    if pow(2,w) != M or pow(2,l) != N:
        #TODO cast error!
        print("Something has gone wrong!")
        return (0,0,0,0)

    a = choice(range(1, M, 2))
    return (pow(2,w)-1,w-l,a)




# Given 2 hash function variables (tuple of a,b,p)
# Determine if they are all pairwise independent
def is_pairwise_independent(h1,h2):
    _,_,a1 = h1
    _,_,a2 = h2
    return a1 != a2 #TODO just one need to be different?


# Given array of hash functions (maybe just the triple (a,b,p) instead?)
# Determine if they are all pairwise independent
# If not, return non-independent hash-functions
def array_is_pairwise_independent(hash_funcs):
    for ind1, h1 in enumerate(hash_funcs):
        for ind2, h2 in enumerate(hash_funcs):
            if h1 != h2:
                if not is_pairwise_independent(h1,h2):
                    return (False,ind1,ind2)
    return (True, -1, -1)

#Returns n pairwise independent hash functions h_i: [M] -> [N]
#M: hash function source space size
#N: hash function target space size
#n: number of hash functions
def get_pw_ind_hash_funcs(n, M, N):
    hs = [get_hash_function(M, N) for i in range(n)]
    b, index_1, _ = array_is_pairwise_independent(hs)
    while not b:
        hs[index_1] = get_hash_function(M, N)
        b, index_1, _ = array_is_pairwise_independent(hs)
    return hs


