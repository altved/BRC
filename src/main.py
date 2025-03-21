# Im using Parallel Chuk file using memory mapping, if anyone cares 
# This is the good resouce to refer -> https://mongard.s3.ir-thr-at1.arvanstorage.com/High%20Performance%20Python%20Practical%20Performant%20Programming%20for%20Humans%20by%20Micha%20Gorelick,%20Ian%20Ozsvald.pdf
# Ch -9 and 11 explain this technique,

import math
import os
import mmap
import concurrent.futures

def ieee_round(x):
    return math.ceil(x * 10) / 10 if x >= 0 else math.floor(x * 10) / 10

def get_chunks(mm, num_chunks):
    file_size = len(mm)
    chunk_size = file_size // num_chunks
    chunks = []
    start = 0
    for i in range(num_chunks):
        if i == num_chunks - 1:
            end = file_size
        else:
            end = start + chunk_size
            newline = mm.find(b'\n', end)
            end = newline if newline != -1 else file_size
        chunks.append((start, end))
        start = end + 1  
    return chunks

def process_chunk_range(args):
    filename, start, end = args
    local_data = {}
    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        mm.seek(start)
        chunk_bytes = mm.read(end - start)
        mm.close()
    

    chunk_str = chunk_bytes.decode("utf-8")
    lines = chunk_str.splitlines()
    
    float_fn = float
    local_data_get = local_data.get
    for line in lines:
        if not line:
            continue
        city, sep, value_str = line.partition(";")
        value = float_fn(value_str)
        try:
            mn, mx, total, count = local_data[city]
            if value < mn:
                mn = value
            if value > mx:
                mx = value
            local_data[city] = (mn, mx, total + value, count + 1)
        except KeyError:
            local_data[city] = (value, value, value, 1)
    return local_data

def merge_dicts(dicts):
    merged = {}
    for d in dicts:
        for city, (mn, mx, total, count) in d.items():
            if city in merged:
                omn, omx, ototal, ocount = merged[city]
                merged[city] = (min(omn, mn), max(omx, mx), ototal + total, ocount + count)
            else:
                merged[city] = (mn, mx, total, count)
    return merged

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    num_cores = os.cpu_count() or 4
    num_chunks = num_cores * 2

    with open(input_file_name, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        chunks = get_chunks(mm, num_chunks)
        mm.close()
    
    args = [(input_file_name, start, end) for start, end in chunks]
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
        results = list(executor.map(process_chunk_range, args))
    
    merged_data = merge_dicts(results)
    
    sorted_cities = sorted(merged_data.keys())
    output_lines = []
    for city in sorted_cities:
        mn, mx, total, count = merged_data[city]
        mean = total / count
        output_lines.append(f"{city}={ieee_round(mn)}/{ieee_round(mean)}/{ieee_round(mx)}\n")
    
    with open(output_file_name, "w") as f:
        f.writelines(output_lines)

if __name__ == "__main__":
    main()
