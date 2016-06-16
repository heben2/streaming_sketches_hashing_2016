#!/bin/env python3
from math import log, ceil, floor, sqrt
from random import choice, randint
import time
import stream

# import hashing as hs
# import modulus_hashing as modh

################## Modulus-prime scheme ######################3
def hash_func_modulus_prime(N,a,p,x):
    return ((a*x) % p) % N

#Can be _very!_ slow for large n.
def is_prime(n):
    if n % 2 == 0 and n > 2: 
        return False
    return all(n % i for i in range(3, int(sqrt(n)) + 1, 2))

#Constructs 2-universal hash function parameters
#M=universe_size, N=target_size
#Returns tuple of tuple of variables and hash function.
def get_hash_function_modulus_prime(M, N):
    p = M + randint(1, N*M*2)
    
    print("Getting primes. Might take a while.")
    while(not is_prime(p)): 
        p += 1
    print("Primes are set.")
    a = randint(1,p-1)
    # b = randint(0,p-1)
    return (N,a,p)
#################################################################
#################### different fast hashing schemes #############
#Return parameters for 2-universal hash function
def get_hash_function_test3(M, N):
    w = ceil(log(M,2))
    l = ceil(log(N,2))

    if pow(2,w) != M or pow(2,l) != N:
        #TODO cast error!
        print("Something has gone wrong!")
        return (0,0,0,0)

    a = choice(range(1, M, 2))
    mask = pow(2,w)-1
    s = w-l
    return (mask,s,a)

#mask to imitate overflow, works like modulus
def two_uni_hash_func3(mask,s,a,x):
    return ((a*x) & mask) >> s

def get_hash_function_test2(M, N):
    w = ceil(log(M,2))
    l = ceil(log(N,2))

    if pow(2,w) != M or pow(2,l) != N:
        #TODO cast error!
        print("Something has gone wrong!")
        return (0,0,0,0)

    a = choice(range(1, M, 2))
    t = pow(2,w)
    s = w-l
    return (t,s,a)

#right shift = int division when power of 2
def two_uni_hash_func2(t,s,a,x):
    return ((a*x) % t) >> s

def get_hash_function_test1(M, N):
    w = ceil(log(M,2))
    l = ceil(log(N,2))

    if pow(2,w) != M or pow(2,l) != N:
        #TODO cast error!
        print("Something has gone wrong!")
        return (0,0,0,0)

    a = choice(range(1, M, 2))
    t = pow(2,w)
    s = pow(2,w-l)
    return (t,s,a)

def two_uni_hash_func1(t,s,a,x):
    return floor( (a*x % t)/s )

##############################################3

'''
Test all the different 2-universal fast hashing schemes.
'''

out_path = 'hashing2_results.txt'

#parameters
M = pow(2,21) #~1 mio
N = pow(2,17) #~130K

h1 = get_hash_function_test1(M, N)
h2 = get_hash_function_test2(M, N)
h3 = get_hash_function_test3(M, N)
h_mod = get_hash_function_modulus_prime(M, N)


#Reusing last hashing functions created.
iterations = M
warmup_iterations = 100



for i in range(warmup_iterations):
    r = two_uni_hash_func1(*h1,i)

start = time.clock()
for i in range(iterations):
    r = two_uni_hash_func1(*h1,i)
end = time.clock()
elapsed_t = end - start
h1_avg = elapsed_t/iterations

for i in range(warmup_iterations):
    r = two_uni_hash_func2(*h2,i)

start = time.clock()
for i in range(iterations):
    r = two_uni_hash_func2(*h2,i)
end = time.clock()
elapsed_t = end - start
h2_avg = elapsed_t/iterations

for i in range(warmup_iterations):
    r = two_uni_hash_func3(*h3,i)

start = time.clock()
for i in range(iterations):
    r = two_uni_hash_func3(*h3,i)
end = time.clock()
elapsed_t = end - start
h3_avg = elapsed_t/iterations


#with prime
for i in range(warmup_iterations):
    r = hash_func_modulus_prime(*h_mod,i)

start = time.clock()
for i in range(iterations):
    r = hash_func_modulus_prime(*h_mod,i)
end = time.clock()
elapsed_t = end - start
h4_avg = elapsed_t/iterations

# #with precomputed pow(2,w)-1 and w-l.
# for i in range(warmup_iterations):
#     r = hs.hash_func(*h_hs,i)

# start = time.clock()
# for i in range(iterations):
#     r = hs.hash_func(*h_hs,i)
# end = time.clock()
# elapsed_t = end - start
# h5_avg = elapsed_t/iterations



stream.output_val_to_file(out_path, "Hashing query/running times", None)
stream.output_val_to_file(out_path, "Iterations:", iterations)
stream.output_val_to_file(out_path, "two_uni_hash_func1 avg:", h1_avg)
stream.output_val_to_file(out_path, "two_uni_hash_func2 avg:", h2_avg)
stream.output_val_to_file(out_path, "two_uni_hash_func3 avg:", h3_avg)
stream.output_val_to_file(out_path, "modulus_hash avg:", h4_avg)
# stream.output_val_to_file(out_path, "precomp values shift+mask avg:", h5_avg)







'''
#Hashing based on Thorups 'High speed hashing for integers and strings' (2015)
#Hash element x according to given parameters a, b.
#w and l defined by hash function [2^w]->[2^l]
#i.e. assumes w and l power of 2.
#The hashing is strongly universal.
'''

#Assumes integers input only
# def strongly_uni_hash_func(w,l,a,b,x):
#     h = (a*x + b) >> (w-l) 
#     #the above is incorrect! Masking is done before shifting.
#     #THIS SHOULD BE  ((a*x + b) & (pow(2,l)-1)) >> (w-l)
#     return h & (pow(2,l)-1) #mask return val to get only l bits
def strongly_uni_hash_func(w,l,a,b,x):
    return (a*x + b) & (pow(2,l)-1) >> (w-l) 

#Hashing h_a,b(j): [2^w] -> {1,-1}
def strongly_uni_hash_func_binary(w,l,a,b,x):
    h = (a*x + b) & (pow(2,l)-1) >> (w-l) 
    return h*2-1 #mask via & to get first bit only


#M=universe_size, N=target_size, both must be power of 2.
#Returns (w_hat,l,a,b) representing a hash function
def get_strongly_universal_hash_func(M,N):
    w = ceil(log(M,2))
    l = ceil(log(N,2))

    if pow(2,w) != M or pow(2,l) != N:
        #TODO cast error!
        print("Something has gone wrong!")
        return (0,0,0,0)

    w_hat = (w + l -1)
    # print("get_hash_function")
    t = pow(2,w_hat)
    a = randint(1,t)
    b = randint(0,t)
    return (w_hat,l,a,b)