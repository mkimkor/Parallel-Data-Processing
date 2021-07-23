# Parallel Search algorithm to search data for specific date.
# Round-Robin partitioning method was chosen for to speed up 
# and binary search method to compare with the key value after sorting.

#Round-Robin partition (equal data partitioning)
def rr_partition(data, n):
    result = []
    for i in range(n):
        result.append([])
        
    n_bin = len(data)/n
        
    for index, element in enumerate(data):
        index_bin = (int) (index % n)
        result[index_bin].append(element)
        
    return result

#Binary_search (suitable for searching one specific value)
def binary_search(data, key):
    matched_record = None
    position = -1
    
    lower = 0
    middle = 0
    upper = len(data) - 1
    
    while(lower <= upper):
        middle = int((lower + upper)/2)
        
        if data[middle][1] == key:
            matched_record = key
            position = middle
            break
        
        else:
            if key < data[middle][1]:
                upper = middle - 1
            else:
                lower = middle + 1
    
    return position


# Parallel search exact value - apply search method after Round-Robin partitioning
from multiprocessing import Pool

def parallel_search_exact(data, query, n_processor, m_partition, m_search):
    results = []
    
    pool = Pool(processes = n_processor)
    
    if m_partition == rr_partition:
        DD = m_partition(data, n_processor)
        for d in DD:
            result = pool.apply(m_search, [d, query])
            if result != -1:
                results.append(d[result])
    return results

# Data implementation. 'Cdata' means climate data.
data = Cdata
sortD = list(data)
sortD.sort()
query = '2017-12-15'
n_processor = 3
result = parallel_search_exact(sortD, '2017-12-15', 3, rr_partition, binary_search)
print(result)



# An algorithm to find data in specific range of temperature.
# Range partition technique and the linear search method are used.
# 'Fdata' means fire data.
def range_partition(data, range_indices):
   
    result = []
    
    new_data = list(data)
    new_data = sorted(new_data, key = lambda x: int(x[7]))
    n_bin = len(range_indices)
    
    for i in range(n_bin):
        s = [x for x in new_data if int(x[7]) < range_indices[i]] # Add the partitioned list to the result 
        result.append(s)
        last_element = s[len(s)-1]
        last = new_data.index(last_element)
        new_data = new_data[int(last)+1:]
        
    result.append([x for x in new_data if int(x[7]) >= range_indices[n_bin-1]])
    return result

def linear_search(data, query): 
    minimum = query[0]
    maximum = query[1]
    matched_record = []
    
    for x in data:
        if (int(x[7]) <= maximum) and (int(x[7]) >= minimum): 
            matched_record.append(x)  
    return matched_record

from multiprocessing import Pool
def parallel_search_range(data, query_range, n_processor): 
   
    results = []
    pool = Pool(processes = n_processor) 
    
    DD = range_partition(data, query_range)
    for d in DD:       
        result = pool.apply(linear_search, [d, query_range]) 
        results.append(result)    
    return results

result = parallel_search_range(Fdata, [65, 100], 3)
print(result)



# Parallel Join - find the matching data from two different dataset.
# The larger data is used for divide and another one is used for broad casting.
# Round-Robin partition and Nested-Loop Join are used.
def NL_join(T1, T2):
    result = []
    
    for tr1 in T1:
        for tr2 in T2:
            if (tr1[6] == tr2[1]):
                result.append({", ".join([tr1[6], tr1[7], tr2[2], tr2[3], tr2[5]])})
                
    return result

# Distributed Data Parallel
import multiprocessing as mp

def DDP_join(T1, T2, n_processor):
    result = []
    
    T1_subsets = rr_partition(T1, n_processor)
    
    pool = mp.Pool(processes = n_processor)
    
    for t1 in T1_subsets:
        result.append(pool.apply(NL_join, [t1, T2]))
        
    return result

n_processor = 3
result = DDP_join(Fdata, Cdata, n_processor)
for x in result:
    result.sort(key = lambda x: x[0])
print(result)


# Parallel Sort - based on the surface temperature in a ascending order.
# Used quick sort locally then finally merge each distributed data using k-way merge.
# Round-Robin partitioning is used for load balancing.
def qsort(arr): 
    
    if len(arr) <= 1:
        return arr
    else:
        return qsort([x for x in arr[1:] if int(x[7]) < int(arr[0][7])]) \
                      + [arr[0]] \
                      + qsort([x for x in arr[1:] if int(x[7]) >= int(arr[0][7])])

import sys
def find_min(records): 
    
    m = records[0]
    index = 0
    for i in range(len(records)):

        if (records[i] == sys.maxsize):
            continue
            
        if (m == sys.maxsize or (int(records[i][7]) < int(m[7]))):
            index = i
            m = records[i]
    return index

def k_way_merge(record_sets):     
    indexes = []
    
    for x in record_sets:
        indexes.append(0) 
    
    result = []   
    tuple = []
        
    while(True):
        tuple = [] 
        
        for i in range(len(record_sets)):
            if(indexes[i] >= len(record_sets[i])):                                                         
                tuple.append(sys.maxsize)                                                               
            else:                                     
                tuple.append(record_sets[i][indexes[i]]) 
                        
        smallest = find_min(tuple)
        if(tuple[smallest] == sys.maxsize):                                                           
            break
            
        result.append(record_sets[smallest][indexes[smallest]])
        indexes[smallest] +=1
                
    return result

def serial_sorting(dataset, buffer_size): 
    if (buffer_size <= 2):
        print("Error: buffer size should be greater than 2") 
        return
    
    result = []
      
    #sort phase
    sorted_set = []
    
    start_pos = 0
    N = len(dataset)
    while True:
        if ((N - start_pos) > buffer_size):
            subset = dataset[start_pos:start_pos + buffer_size]
            sorted_subset = qsort(subset)
            sorted_set.append(sorted_subset)
            start_pos += buffer_size
        else:
            subset = dataset[start_pos:]
            sorted_subset = qsort(subset) 
            sorted_set.append(sorted_subset)
            break
                
    #merge phase
    merge_buffer_size = buffer_size - 1 
    dataset = sorted_set
    while True:
        merged_set = []
        
        N = len(dataset) 
        start_pos = 0
        while True:
            if ((N - start_pos) > merge_buffer_size):
                subset = dataset[start_pos:start_pos + merge_buffer_size] 
                merged_set.append(k_way_merge(subset)) 
                start_pos += merge_buffer_size

            else:
                subset = dataset[start_pos:] 
                merged_set.append(k_way_merge(subset)) 

                break
            
        dataset = merged_set
        if (len(dataset) <= 1): 
            result = merged_set
            break
    return result

def rr_partition(data, n): 
    
    result = []
    for i in range(n):
        result.append([])
            
    n_bin = len(data)/n
    
    for index, element in enumerate(data):
        index_bin = (int) (index % n)
        result[index_bin].append(element) 
            
    return result

import multiprocessing as mp

def parallel_merge_all_sorting(dataset, n_processor): 
    
    result = []
        
    subsets = rr_partition(dataset, n_processor)
    
    pool = mp.Pool(processes = n_processor) 
    
    #sort phase     
    sorted_set = []
    for s in subsets:
        sorted_set.append(*pool.apply(serial_sorting, [s, 5]))
    pool.close()
    
    #merge phase
    result = k_way_merge(sorted_set)   
    
    return result

result = parallel_merge_all_sorting(Fdata, 4)
print(result)



# Parallel Group-By
# Count a record on each date then display the total number.
# Local Group-By applied for each processor concurrently then merged
# to process a global Group-By method.
def local_groupbyDate(dataset):
    
    dict = {}
    for x in dataset:
        key = x[6]
        if key in dict:
            dict[key] += 1
        else:
            dict[key] = 1
        
    return dict

import multiprocessing as mp

def parallel_merge_all_groupbyDate(dataset):
    
    result = {}
    
    n_processor = len(dataset)
    
    pool = mp.Pool(processes = n_processor)
    
    local_result = []
    for s in dataset:
        local_result.append(pool.apply(local_groupbyDate, [s]))
    pool.close()
    
    for r in local_result:
        for key, val in r.items():
            if key not in result:
                result[key] = 0
            result[key] += val
            
    return result

data = rr_partition(Fdata, 4)
result = parallel_merge_all_groupbyDate(data)
for key in sorted(result):
    print (key, result[key])



# Parallel Group-By Join
# Date is the join key between two different data sets.
# The join process will end when it reaches the last index of the larger dataset.
def date_range_partition(data, range_indices, col_n):
    
    result = []

    new_data = list(data)
    new_data.sort(key = lambda x: x[col_n])

    n_bin = len(range_indices)

    for i in range(n_bin):
        s = [x for x in new_data if x[col_n][:7] < range_indices[i]]
        result.append(s)
        last_element = s[len(s) - 1]
        last = new_data.index(last_element)
        new_data = new_data[int(last) + 1:]

    result.append([x for x in new_data if x[col_n][:7] >= range_indices[n_bin - 1]])

    return result

#Sort-Merge Join
def date_SM_join(T1, T2):
    
    result = []
    
    i = j = 0
    while True:
        r = T1[i][1]
        s = T2[j][6]
        
        if r < s:
            i += 1
            
        elif r > s:
            j += 1
        
        else:
            result.append({", ".join([str(T1[i][1]), T1[i][0], T2[j][7]])})
            j += 1
        
        if (j == len(T2)):
            break
            
    return result

# Disjoint Partitioning
import multiprocessing as mp

def DPBP_join(T1, T2, n_processor):
    
    result = []
    
    T1_subsets = date_range_partition(T1, ['2017-05', '2017-08'], 1)
    T2_subsets = date_range_partition(T2, ['2017-05', '2017-08'], 6)
    
    pool = mp.Pool(processes = n_processor)
    
    for i in range(len(T2_subsets)):
        result.append(pool.apply(date_SM_join, [T1_subsets[i], T2_subsets[i]]))
        
    return result

def groupby_station(dataset):
    
    result = {}
    
    dict = {}
    for ranges in dataset:
        for subset in ranges:
            for string in subset:
                key = string[12:18]
                val = int(string[20:])
                if key not in dict:
                    dict[key] = [0, 0, 0]
                dict[key][0] += val
                dict[key][1] += 1
                dict[key][2] = dict[key][0] / dict[key][1]

    for key, val in dict.items():
        if key not in result:
            result[key] = 0
        result[key] = dict[key][2]
        
    return result

n_processor = 3
data = DPBP_join(Cdata, Fdata, n_processor)
result = groupby_station(data)
print(result)



