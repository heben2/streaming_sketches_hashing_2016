#!/bin/env python3

from random import random
from math import ceil, log
import operator
import time

from hashing import *
import stream
from stream import parse_token
from count_sketch import *
from count_min_sketch import *
#from count_sketch import *

dump_size_min_power = 0
dump_size_max_power = 0
p = 0

count_sketch_result_path = "count_sketch_output.txt"
count_sketch_median_result_path = "count_sketch_median_output.txt"
count_min_sketch_result_path = "count_min_sketch_output.txt"
count_min_median_sketch_result_path = "count_min_median_sketch_output.txt"

count_sketch_perf_result_path = "performance_count_sketch_output.txt"
count_sketch_median_perf_result_path = "performance_count_sketch_median_output.txt"
count_min_sketch_perf_result_path = "performance_count_min_sketch_output.txt"
count_min_median_sketch_perf_result_path = "performance_count_min_median_sketch_output.txt"


def set_dump_size_min_power(x):
    global dump_size_min_power
    dump_size_min_power = x

def set_dump_size_max_power(x):
    global dump_size_max_power
    dump_size_max_power = x

def set_p(x):
    global p
    p = x

#Experiments must be run on k a power of 2. As the results should make nice 
#graphs, we don't want to make k or error e random, but run over a range.
def get_next_k(k_old):
    #global k_count_sketch, k_count_min_sketch
    if k_old < 1:
        return 1
    return k_old*2


#Compares two lists of items
#Returns number of different items between both, 
#i.e.the symmetric difference
#Lower is better, 0 is equal sets
def compare_items(items1, items2):
    i1 = frozenset(items1) #could use set, but these are now static
    i2 = frozenset(items2)
    print("Compare items")
    print("num frequent items in true   = " + str(len(i1)))
    print("num frequent items in sketch = " + str(len(i2)))
    i3 = i1 ^ i2 #symmetric_difference
    return len(i3)


#items = [(item, frequency)]
#assumes item is char
def print_frequent_items(items):
    print("Result set:\n[")
    for a,f in items:
        print("(elem,freq) = ("+str(a)+","+str(f)+")")
    print("]")


#Return most frequent items, e.g. items with frequence > m/k
def get_frequent_items(frequencies, m, k):
    # print("m = " + str(m))
    # print("k = " + str(k))
    # print("m/k = " + str(m/k))
    if k <= 0:
        return [] #Throw exception
    return [(a,f) for (a,f) in frequencies if f > m/k]



#Returns (average,variance) of given data list
def get_avg_var(data_list):
    N = len(data_list)
    avg = sum(data_list)/N
    var = sum([pow(e-avg,2) for e in data_list])/N
    return avg, var


def get_avg_var_double_list(data_list):
    N = len(data_list[0])
    M = len(data_list)
    avg_list = [sum(e)/N for e in data_list]
    var_list = [sum([pow(e-avg_list[i],2) for e in data_list[i]])/N \
                for i in range(M)]
    return avg_list, var_list


#Compute the true frequencies of input_stream
def get_true_frequencies(input_stream, token_set):
    result = {parse_token(a):0 for a in token_set}
    a,c = input_stream.next()
    while a != '':
        result[parse_token(a)] += c
        a,c = input_stream.next()
    return sorted(result.items(), key=operator.itemgetter(0))



#run given sketch on input_stream with given token_set.
#return list of item-frequency pairs for all tokens in token_set.
def run_sketch(sketch, input_stream, token_set):
    a,c = input_stream.next()
    while a != '':
        sketch.process(parse_token(a),c)
        a,c = input_stream.next()
    return [(parse_token(a),sketch.query(parse_token(a))) for a in token_set]
   

def process_sketch(sketch, input_stream, token_set):
    a,c = input_stream.next()
    while a != '':
        sketch.process(parse_token(a),c)
        a,c = input_stream.next()


def query_all_tokens_sketch(sketch, token_set):
    return [(a,sketch.query(a)) for a in token_set]


# def test_true_result(ids, token_set, k):
#     print("Compute true frequencies")
#     # frequencies = get_true_frequencies(ids, token_set)
#     print("m = " + str(ids.m))
#     print("minimum frequency m/k = " + str(int(ids.m/k)))
#     items = get_frequent_items(frequencies, ids.m, k)
#     # print_frequent_items(items) #DEBUGGING
#     return items


#Run a given sketch (object) on given input_stream with given token_set for 
#parameter k. Compare results to true frequencies (also runned here)
#Return time taken for preprocessing and querying
def test_sketch(input_stream, token_set, true_frequencies, sketch, k):
    frequencies = run_sketch(sketch, input_stream, token_set)

    item_freq_list = get_frequent_items(frequencies, input_stream.m, k)
    true_item_freq_list = get_frequent_items(true_frequencies, input_stream.m, 
                                             k)
    m = input_stream.m
    num_items = input_stream.i
    print("m = " + str(m))
    print("k = " + str(k))
    print("m/k = " + str(input_stream.m/k))

    # print("start_query = "+str(start_query)+", end_query = "+str(end_query))
    
    # print_frequent_items(item_freq_list) #DEBUGGING

    #DEBUGGING
    # print(true_item_freq_list)
    # print(item_freq_list)
    ########

    true_items = [i[0] for i in true_item_freq_list]
    sketch_items = [i[0] for i in item_freq_list]
    item_diff = compare_items(true_items, sketch_items)
    print("Item difference = " + str(item_diff))
    num_true_freq_items = len(true_items)
    num_sketch_freq_items = len(sketch_items)
    input_stream.reset()
    return item_diff, num_true_freq_items, num_sketch_freq_items, m, num_items


# Assumes k_bound is power of 2. Will construct token universe of size k_bound*4
def run_test_over_k(input_stream, token_set, true_frequencies, k_bound, k_min,
             sketch_fun, sketch_delta=-1.):
    # input_stream_path = "dump.json"
    # token_path = "tokens.txt"
    # token_set = stream.get_token_set(token_path)
    # input_stream = stream.Stream(input_stream_path)
    #universe_size = k_bound*4 #Must be power of 2 and larger than k
    universe_size = len(token_set)

    # init_k_power = min(5,dump_size_min_power-1) #arbitrarily low k chosen
    init_k_power = floor(log(k_min,2))
    # k_min = pow(2,init_k_power)

    k_iterations = int(floor(log(k_bound,2))) - init_k_power
    k_iterations += 0 if k_bound % 2 == 0 else 1

    item_diffs = [-1]*k_iterations

    k = k_min
    while k < k_bound: #Run for same stream but different k
        if sketch_delta > 0:
            sketch = sketch_fun(universe_size, k, sketch_delta)
        else:
            sketch = sketch_fun(universe_size, k)
        item_diff, num_true_freq_items, num_sketch_freq_items, m, num_items = \
            test_sketch(input_stream, token_set, true_frequencies, sketch, k)
        j = int(log(k,2))-init_k_power
        item_diffs[j] = {"diff": item_diff, 
                        "true_freq_items":num_true_freq_items, 
                        "sketch_freq_items":num_sketch_freq_items,
                        "m":m,
                        "#items": num_items}
        k = get_next_k(k)
        #Debugging/progress printing
        print("item diffs:")
        print(item_diffs)
    
    avg_item_diffs, var_item_diffs = get_avg_var([i['diff'] for i in 
                                                 item_diffs])

    # avg_item_diffs, var_item_diffs = get_avg_var_double_list(item_diffs) #TODO

    return item_diffs, avg_item_diffs, var_item_diffs

#truncate result file and fill in general data
def init_result_file(out_path, k_min, k_max, d=-1):
    trunc_file(out_path)
    if d > 0:
        stream.output_val_to_file(out_path, "delta", d)
    stream.output_val_to_file(out_path, "k_min", k_min)
    stream.output_val_to_file(out_path, "k_max", k_max)
    stream.output_val_to_file(out_path, "dump_size_min_power", 
                              dump_size_min_power)
    stream.output_val_to_file(out_path, "dump_size_max_power", 
                              dump_size_max_power)


#Prints results do file of out_path
def output_results(out_path, dump_size, item_diffs, avg_item_diffs, 
                   var_item_diffs, d=-1):
    # stream.output_dlist_to_file(out_path, "item diffs:", item_diffs)
    stream.output_val_to_file(out_path, "current dump size:", dump_size)

    stream.output_list_to_file(out_path, "item diffs:", item_diffs)
    stream.output_list_to_file(out_path, "avg item diff:", avg_item_diffs)
    stream.output_list_to_file(out_path, "var item diff:", var_item_diffs)

#Run all sketches on same dump over range of dump sizes
def loop_dumps(k_bound, k_min, universe_size, construct_dump_func, d):
    input_stream_path = "dump.json"
    token_path = "tokens.txt"

    stream.construct_tokens(token_path, universe_size)

    # k_iterations = int(floor(log(k_bound,2))) - init_k_power
    k_iterations = floor(log(k_bound,2))
    k_iterations += 0 if k_bound % 2 == 0 else 1
    k_max = pow(2,int(k_iterations))

    init_result_file(count_sketch_result_path, k_min, k_max)
    init_result_file(count_sketch_median_result_path, k_min, k_max, d)
    init_result_file(count_min_sketch_result_path, k_min, k_max, d)
    init_result_file(count_min_median_sketch_result_path, k_min, k_max, d)
    for x in range(dump_size_min_power,dump_size_max_power): 
        dump_size = int(pow(2,x))
        construct_dump_func(input_stream_path,token_path,dump_size, p)
        token_set = stream.get_token_set(token_path)
        input_stream = stream.Stream(input_stream_path)
        #compute true frequencies
        print("Dump size = " + str(dump_size))
        true_frequencies = get_true_frequencies(input_stream, token_set)
        input_stream.reset()

        # Count sketch
        results = run_test_over_k(input_stream, token_set, true_frequencies, 
                                  k_bound, k_min, CountSketch)
        output_results(count_sketch_result_path, dump_size, *results)
        # Count sketch median (final)
        results = run_test_over_k(input_stream, token_set, true_frequencies, 
                                  k_bound, k_min, CountSketchMedian, d)
        output_results(count_sketch_median_result_path, dump_size, *results, d)
        # Count min sketch
        results = run_test_over_k(input_stream, token_set, true_frequencies, 
                                  k_bound, k_min, CountMinSketch, d)
        output_results(count_min_sketch_result_path, dump_size, *results, d)
        #Count min median sketch
        results = run_test_over_k(input_stream, token_set, true_frequencies, 
                                  k_bound, k_min, CountMinMedianSketch, d)
        output_results(count_min_median_sketch_result_path, dump_size, *results, 
                       d)


def run_performance_dump_size(sketch_fun, d, k, dump_p_min, dump_p_max, 
                              dump_p_interval, test_interval, 
                              construct_dump_func, token_set):
    input_stream_path = "dump.json"
    token_path = "tokens.txt"
    universe_size = len(token_set)

    runs = ceil((dump_p_max - dump_p_min)/dump_p_interval)
    t_preprocs = [{'avg':-1,'var':-1} for j in range(runs)]
    t_queries = [{'avg':-1,'var':-1} for j in range(runs)]

    i = 0 #easier than computing index off dp
    for dp in range(dump_p_min, dump_p_max, dump_p_interval):
        #create input stream
        dump_size = int(pow(2,dp))
        construct_dump_func(input_stream_path,token_path,dump_size)
        input_stream = stream.Stream(input_stream_path)

        #############################
        print("Do warm up runs")
        for l in range(ceil(test_interval/2)):
            if d > 0:
                sketch = sketch_fun(universe_size, k, d)
            else:
                sketch = sketch_fun(universe_size, k)
            process_sketch(sketch, input_stream, token_set)
            input_stream.reset()
        ##############################
        tmp_preprocs = [-1]*test_interval
        tmp_queries = [-1]*test_interval
        for l in range(test_interval):
            if d > 0:
                sketch = sketch_fun(universe_size, k, d)
            else:
                sketch = sketch_fun(universe_size, k)

            start_preproc = time.clock()
            process_sketch(sketch, input_stream, token_set)
            end_preproc = time.clock()
            elapsed_preproc = end_preproc - start_preproc

            start_query = time.clock()
            _ = query_all_tokens_sketch(sketch, token_set)
            end_query = time.clock()
            elapsed_query = end_query - start_query

            tmp_preprocs[l] = elapsed_preproc
            tmp_queries[l] = elapsed_query
            input_stream.reset()
        avg,var = get_avg_var(tmp_preprocs)
        t_preprocs[i]['avg'] = avg
        t_preprocs[i]['var'] = var
        avg,var = get_avg_var(tmp_queries)
        t_queries[i]['avg'] = avg
        t_queries[i]['var'] = var
        ##debugging/progress indicator
        print(t_preprocs)
        print(t_queries)
        ##############################
        i += 1

    return t_preprocs, t_queries


#Set d_min = d_bound=0 to run sketches for no delta.
#Tests only performance, ignores difference in items
def run_performance_d_k(sketch_fun, d_bound, d_min, d_interval, k_bound, k_min, 
                        test_interval, input_stream, token_set):
    universe_size = len(token_set)
    d = d_min
    k_iterations = floor(log(k_bound,2)) - floor(log(k_min,2))
    k_iterations += 0 if k_bound % 2 == 0 else 1


    #inner list is for average and variance from test_interval runs
    d_range = ceil((d_bound-d_min)/d_interval) + 1 if d_interval > 0 else 1
    t_preprocs = [[{'avg':-1,'var':-1} for j in range(k_iterations)] 
                  for l in range(d_range)]
    t_queries = [[{'avg':-1,'var':-1} for j in range(k_iterations)] 
                  for l in range(d_range)]

    #############################
    print("Do warm up runs")
    for l in range(test_interval):
        if d > 0:
            sketch = sketch_fun(universe_size, k_min, d)
        else:
            sketch = sketch_fun(universe_size, k_min)
        process_sketch(sketch, input_stream, token_set)
        input_stream.reset()
    ##############################
    print("Total num iterations = " + str(k_iterations*d_range*test_interval))
    #indices for convinience
    i = 0
    while d <= d_bound:
        j = 0
        k = k_min
        while k < k_bound:
            tmp_preprocs = [-1]*test_interval
            tmp_queries = [-1]*test_interval
            for l in range(test_interval):
                if d > 0:
                    sketch = sketch_fun(universe_size, k, d)
                else:
                    sketch = sketch_fun(universe_size, k)

                start_preproc = time.clock()
                process_sketch(sketch, input_stream, token_set)
                end_preproc = time.clock()
                elapsed_preproc = end_preproc - start_preproc

                start_query = time.clock()
                _ = query_all_tokens_sketch(sketch, token_set)
                end_query = time.clock()
                elapsed_query = end_query - start_query

                tmp_preprocs[l] = elapsed_preproc
                tmp_queries[l] = elapsed_query
                m = input_stream.m
                input_stream.reset()
            avg,var = get_avg_var(tmp_preprocs)
            t_preprocs[i][j]['avg'] = avg
            t_preprocs[i][j]['var'] = var
            avg,var = get_avg_var(tmp_queries)
            t_queries[i][j]['avg'] = avg
            t_queries[i][j]['var'] = var
            k = get_next_k(k)
            j += 1
            ##debugging/progress indicator
            print(t_preprocs)
            print(t_queries)
            ##############################
        d += d_interval if d_interval > 0 else 1 #aka break
        i += 1

    return t_preprocs, t_queries, m


#Prints results do file of out_path
def output_perf_results(out_path, dump_size, t_preprocs, t_queries, d_min, 
                        d_bound, d_interval, k_min, k_bound, test_interval):
    stream.output_val_to_file(out_path, "current dump size:", dump_size)
    stream.output_val_to_file(out_path, "d_min:", d_min)
    stream.output_val_to_file(out_path, "d_bound:", d_bound)
    stream.output_val_to_file(out_path, "d_interval:", d_interval)
    stream.output_val_to_file(out_path, "k_min:", k_min)
    stream.output_val_to_file(out_path, "k_bound:", k_bound)
    stream.output_val_to_file(out_path, "test_interval:", test_interval)
    stream.output_val_to_file(out_path,
                              "preproc stored by [[loop on k]loop on d]", None)
    stream.output_list_to_file(out_path, "preproc times, var and avg:", 
                               t_preprocs)
    stream.output_list_to_file(out_path, "query times, var and avg:", t_queries)


def output_perf_dump_results(out_path, d, k, t_preprocs, t_queries, dump_p_min, 
                            dump_p_max, dump_p_interval, test_interval):
    stream.output_val_to_file(out_path, "delta d:", d)
    stream.output_val_to_file(out_path, "k:", k)
    stream.output_val_to_file(out_path, "dump size power min:", dump_p_min)
    stream.output_val_to_file(out_path, "dump size power max:", dump_p_max)
    stream.output_val_to_file(out_path, "dump size power interval:",
                              dump_p_interval)
    stream.output_val_to_file(out_path, "test_interval:", test_interval)
    stream.output_val_to_file(out_path,
                              "preproc stored by [[loop on k]loop on d]", None)
    stream.output_list_to_file(out_path, "preproc times, var and avg:", 
                               t_preprocs)
    stream.output_list_to_file(out_path, "query times, var and avg:", t_queries)


def test_correctness():
    '''
    The dump sizes are 2 to the power of dump size power.

    If stream.construct_uniform_dump is used, the dump size is uniformly 
    distributed on all tokens, that is, the total count on all items in the 
    stream is the dump size.

    If stream.construct_std_norm_dist_dump is used, then it determines how many
    tokens will assigned a count >= 1. The count is based on the standard normal 
    distribution in an exponential fashion, where each count is assigned as 
    ceil(pow(2,abs(i)*3) for i in the distribution. 
    Note each count is assigned to a random token (including all previous 
    assigned).

    #dump_size: 30 ~= 1 bil, 20 ~= 1 mil

    Note that dump sizes are used as range, and so the upper bound is not 
    included. Thus it makes no sense to use same min and max dump size power!
    '''
    # set_dump_size_min_power(15)
    # set_dump_size_max_power(16) #debugging

    set_dump_size_min_power(20)
    set_dump_size_max_power(25) #~16 mil used (24 is effective cap)
    # set_dump_size_max_power(27) #~65 mil used (26 is effective cap)
    # set_dump_size_min_power(5)
    # set_dump_size_max_power(7)
    # set_dump_size_min_power(23)
    # set_dump_size_max_power(24)
    '''
    k_bound
    If power of 2, we can compute min source universe size directly
    Universe size is determined by k_bound*4, as it must be larger than k for 
    sketches to make sense.
    Note that the runs are to first k power of 2 strictly larger than k_bound
    (to easily handle k_bounds not power of 2)
    '''
    k_bound_power = dump_size_min_power-1 #does not make sense to be equal to dump size.
    k_bound = int(pow(2,k_bound_power)) #Should be set global like dump_size_min/max_power
    '''
    Delta d, the error probability parameter.
    '''
    d = 0.001 #TODO try another d, d=0.0001 and/or d=0.01

    b = 1
    if b == 0:
        #Test for evenly distributed frequencies:
        construct_dump_func = stream.construct_uniform_dump
    elif b == 1:
        #Test for standard normal distributed frequencies
        construct_dump_func = stream.construct_std_norm_dist_dump

    universe_size = k_bound*4 #Must be power of 2 and larger than k
    min_k_power = k_bound_power-5 #5 ish iterations over k
    k_min = pow(2,min_k_power)

    loop_dumps(k_bound, k_min, universe_size, construct_dump_func, d)



def test_performance_over_d_k():
    d_bound = 0.01
    d_min = 0.002
    d_interval = 0.002
    k_bound = int(pow(2,20)) #effective bound power is 19
    k_min = int(pow(2,16))
    test_interval = 5 #run test_interval times for each d and k

# ### DEBUGGING
#     d_bound = 0.01
#     d_min = 0.006
#     d_interval = 0.002
#     k_bound = int(pow(2,15))
#     k_min = int(pow(2,14))
#     test_interval = 1
# #############

    universe_size = k_bound*4
    #to get input size linear to dump_size, use construct_uniform_dump
    construct_dump_func = stream.construct_uniform_dump
    dump_size = int(pow(2,20)) #~1 mil

    print("Constructing dump")
    input_stream_path = "dump.json"
    token_path = "tokens.txt"
    # token_set = stream.construct_tokens(token_path, universe_size)
    token_set = get_token_set(token_path)
    # construct_dump_func(input_stream_path,token_path,dump_size)
    input_stream = stream.Stream(input_stream_path)

    print("Running Count Sketch")
    t_preprocs, t_queries, m = run_performance_d_k(CountSketch, 0, 0, 0, k_bound, 
                                               k_min, test_interval, 
                                               input_stream, token_set)
    output_perf_results(count_sketch_perf_result_path, m, t_preprocs, t_queries, 
                        0, 0, 0, k_min, k_bound, test_interval)
    
    print("Running Count Sketch Median")
    t_preprocs, t_queries, m = run_performance_d_k(CountSketchMedian, d_bound, 
                                               d_min, d_interval, k_bound, 
                                               k_min, test_interval, 
                                               input_stream, token_set)
    output_perf_results(count_sketch_median_perf_result_path, m, t_preprocs, 
                        t_queries, d_min, d_bound, d_interval, k_min, k_bound, 
                        test_interval)

    print("Running Count Min Sketch")
    t_preprocs, t_queries, m = run_performance_d_k(CountMinSketch, d_bound, 
                                               d_min, d_interval, k_bound, 
                                               k_min, test_interval, 
                                               input_stream, token_set)
    output_perf_results(count_min_sketch_perf_result_path, m, t_preprocs, 
                        t_queries, d_min, d_bound, d_interval, k_min, k_bound, 
                        test_interval)

    print("Running Count Min Median Sketch")
    t_preprocs, t_queries, m = run_performance_d_k(CountMinMedianSketch, d_bound, 
                                               d_min, d_interval, k_bound, 
                                               k_min, test_interval, 
                                               input_stream, token_set)

    output_perf_results(count_min_median_sketch_perf_result_path, m, 
                        t_preprocs, t_queries, d_min, d_bound, d_interval, 
                        k_min, k_bound, test_interval)



def test_performance_over_dumps():
    d = 0.01
    k = int(pow(2,15))
    test_interval = 5 #run test_interval times for each d and k


    universe_size = k*4
    #to get input size linear to dump_size, use construct_uniform_dump
    construct_dump_func = stream.construct_uniform_dump
    dump_p_min = 16
    dump_p_max = 24
    dump_p_interval = 1

    token_path = "tokens.txt"
    token_set = stream.construct_tokens(token_path, universe_size)
    token_set = get_token_set(token_path)

    print("Running Count Sketch")
    t_preprocs, t_queries = \
        run_performance_dump_size(CountSketch, 0, k, dump_p_min, dump_p_max, 
                              dump_p_interval, test_interval, 
                              construct_dump_func, token_set)
    output_perf_dump_results(count_sketch_perf_result_path, 0, k, 
                        t_preprocs, t_queries, dump_p_min, dump_p_max, 
                        dump_p_interval, test_interval)
    
    print("Running Count Sketch Median")
    t_preprocs, t_queries = \
    run_performance_dump_size(CountSketchMedian, d, k, dump_p_min, dump_p_max, 
                              dump_p_interval, test_interval, 
                              construct_dump_func, token_set)

    output_perf_dump_results(count_sketch_median_perf_result_path, d, k, 
                        t_preprocs, t_queries, dump_p_min, dump_p_max, 
                        dump_p_interval, test_interval)

    print("Running Count Min Sketch")
    t_preprocs, t_queries = \
    run_performance_dump_size(CountMinSketch, d, k, dump_p_min, dump_p_max, 
                              dump_p_interval, test_interval, 
                              construct_dump_func, token_set)

    output_perf_dump_results(count_min_sketch_perf_result_path, d, k, 
                        t_preprocs, t_queries, dump_p_min, dump_p_max, 
                        dump_p_interval, test_interval)

    print("Running Count Min Median Sketch")
    t_preprocs, t_queries = \
    run_performance_dump_size(CountMinMedianSketch, d, k, dump_p_min, 
                              dump_p_max, dump_p_interval, test_interval, 
                              construct_dump_func, token_set)

    output_perf_dump_results(count_min_median_sketch_perf_result_path, d, k, 
                        t_preprocs, t_queries, dump_p_min, dump_p_max, 
                        dump_p_interval, test_interval)



# TODO Stability:
# For fixed parameters, how well do sketches perform on 
# - different input streams and on same input stream multiple times.
# Note this includes new hashing functions per run, even on same stream.



def run_stability_diff_dumps(sketch_fun, token_set, construct_dump_func, dump_p, iterations, 
                  k, d=0):
    input_stream_path = "dump.json"
    token_path = "tokens.txt"
    universe_size = len(token_set)
    dump_size = int(pow(2,dump_p))

    item_diffs = [-1]*iterations

    for i in range(iterations): 
        construct_dump_func(input_stream_path,token_path,dump_size,p)
        input_stream = stream.Stream(input_stream_path)
        true_frequencies = get_true_frequencies(input_stream, token_set)
        input_stream.reset()

        if d > 0:
            sketch = sketch_fun(universe_size, k, d)
        else:
            sketch = sketch_fun(universe_size, k)
        item_diff, num_true_freq_items, num_sketch_freq_items, m, num_items = \
            test_sketch(input_stream, token_set, true_frequencies, sketch, k)
        item_diffs[i] = {"diff": item_diff, 
                        "true_freq_items":num_true_freq_items, 
                        "sketch_freq_items":num_sketch_freq_items,
                        "m":m,
                        "#items":num_items}
        #Debugging/progress printing
        print("item diffs:")
        print(item_diffs)
    
    avg_item_diffs, var_item_diffs = get_avg_var([i['diff'] for i in 
                                                 item_diffs])

    return item_diffs, avg_item_diffs, var_item_diffs

def run_stability_same_dump(sketch_fun, token_set, input_stream, 
                            true_frequencies, iterations, k, d=0):
    
    universe_size = len(token_set)
    item_diffs = [-1]*iterations

    #using same dump, see results for same sketch multiple times
    for i in range(iterations): 
        input_stream.reset()

        if d > 0:
            sketch = sketch_fun(universe_size, k, d)
        else:
            sketch = sketch_fun(universe_size, k)
        item_diff, num_true_freq_items, num_sketch_freq_items, m, num_items = \
            test_sketch(input_stream, token_set, true_frequencies, sketch, k)
        item_diffs[i] = {"diff": item_diff, 
                        "true_freq_items":num_true_freq_items, 
                        "sketch_freq_items":num_sketch_freq_items,
                        "m":m,
                        "#items":num_items}
        #Debugging/progress printing
        print("item diffs:")
        print(item_diffs)
    
    avg_item_diffs, var_item_diffs = get_avg_var([i['diff'] for i in 
                                                 item_diffs])
    input_stream.reset()
    return item_diffs, avg_item_diffs, var_item_diffs





def print_results_stability(path, d, k, dump_p, iterations, item_diffs, 
                            avg_item_diffs, var_item_diffs, description):
    stream.output_val_to_file(path, "Description:", description)
    stream.output_val_to_file(path, "delta d:", d)
    stream.output_val_to_file(path, "k:", k)
    stream.output_val_to_file(path, "dump power:", dump_p)
    stream.output_val_to_file(path, "iterations:", iterations)
    stream.output_list_to_file(path, "item diffs:", item_diffs)
    stream.output_list_to_file(path, "avg item diff:", avg_item_diffs)
    stream.output_list_to_file(path, "var item diff:", var_item_diffs)

def test_stability():
    input_stream_path = "dump.json"
    token_path = "tokens.txt"
    d = 0.001
    k = int(pow(2,18))
    universe_size = int(pow(2,20))
    token_set = stream.construct_tokens(token_path, universe_size)
    construct_dump_func = stream.construct_std_norm_dist_dump_m
    dump_p = 20

    iterations = 100
    
    # descr = "diff dumps/streams per iteration and sketch"
    # print("Running Count Sketch")
    # results = run_stability_diff_dumps(CountSketch, token_set, construct_dump_func, dump_p,
    #                         iterations, k, 0)
    # print_results_stability(count_sketch_result_path, d, k, dump_p, iterations, 
    #                         *results, descr)

    # print("Running Count Sketch Median")
    # results = run_stability_diff_dumps(CountSketchMedian, token_set, construct_dump_func, 
    #                         dump_p, iterations, k, 0)
    # print_results_stability(count_sketch_median_result_path, d, k, dump_p, iterations, 
    #                         *results, descr)

    # print("Running Count-Min Sketch")
    # results = run_stability_diff_dumps(CountMinSketch, token_set, construct_dump_func, 
    #                         dump_p, iterations, k, 0)
    # print_results_stability(count_min_sketch_result_path, d, k, dump_p, iterations, 
    #                         *results, descr)

    # print("Running Count-Median Sketch")
    # results = run_stability_diff_dumps(CountMinMedianSketch, token_set,
    #                         construct_dump_func, dump_p, iterations, k, 0)
    # print_results_stability(count_min_median_sketch_result_path, d, k, dump_p, iterations, 
    #                         *results, descr)

# ##############################################
    descr = "Same dump/stream per iteration (not per sketch)"

    construct_dump_func(input_stream_path,token_path, pow(2,dump_p))
    input_stream = stream.Stream(input_stream_path)
    true_frequencies = get_true_frequencies(input_stream, token_set)

    print("Running Count Sketch")
    results = run_stability_same_dump(CountSketch, token_set, input_stream, 
                                      true_frequencies, iterations, k, 0)
    print_results_stability(count_sketch_result_path, d, k, dump_p, iterations, 
                            *results, descr)

    print("Running Count Sketch Median")
    results = run_stability_same_dump(CountSketchMedian, token_set, input_stream, 
                                      true_frequencies, iterations, k, 0)
    print_results_stability(count_sketch_median_result_path, d, k, dump_p, 
                            iterations, *results, descr)

    print("Running Count-Min Sketch")
    results = run_stability_same_dump(CountMinSketch, token_set, input_stream, 
                                      true_frequencies, iterations, k, 0)
    print_results_stability(count_min_sketch_result_path, d, k, dump_p, 
                            iterations, *results, descr)

    print("Running Count-Median Sketch")
    results = run_stability_same_dump(CountMinMedianSketch, token_set,
                            input_stream, true_frequencies,  iterations, k, 0)
    print_results_stability(count_min_median_sketch_result_path, d, k, dump_p, 
                            iterations, *results, descr)
    



def main():
    '''
    Go to each of these methods to set parameters.
    Out-comment methods not used in this test run.
    '''
    # test_correctness()

    test_stability()

    # test_performance_over_d_k()

    # test_performance_over_dumps()



if __name__ == '__main__':
    main()