import concurrent.futures
# Read data from FB5M
fb5m_folder = "FB2M"
fb5m_files = ["annotated_fb_data_test.txt", "annotated_fb_data_train.txt", "annotated_fb_data_valid.txt"]

fb5m_data = set()


for file in fb5m_files:
    with open(f"{fb5m_folder}/{file}", "r",encoding="utf8") as f:
        lines = f.readlines()
        for line in lines:
            fb5m_data.add(tuple(line.strip().split("\t")[:4]))  # Extract only the triple (ignore the question)
        print(len(fb5m_data))
fb15k_data = set()
with open("Hugging Face FB15k-237/train.txt", "r",encoding="utf8") as f:
    lines = f.readlines()
    for line in lines:
        fb15k_data.add(tuple(line.strip().split("\t")))

print(len(fb15k_data))

# Create a function to check for common triples
def find_common_triples(fb15k_chunk, fb5m_data, common_data,chunk):
    i=0
    for fb15k_triple in fb15k_chunk:
        i+=1
        print(" ",chunk,":",i," ",end="\r")
        for fb5m_triple in fb5m_data:
            if fb15k_triple[0] in fb5m_triple[0] and fb15k_triple[1] in fb5m_triple[1] and fb15k_triple[2] in fb5m_triple[2]:
                common_data.add(fb5m_triple)

# Split fb15k_data into chunks for parallel processing
chunk_size = 10000
fb15k_chunks = [list(fb15k_data)[i:i + chunk_size] for i in range(0, len(fb15k_data), chunk_size)]

print(len(fb15k_chunks))

common_data = set()
with concurrent.futures.ThreadPoolExecutor(max_workers=102) as executor:
    futures = []
    chunk_no=0
    for chunk in fb15k_chunks:
        chunk_no+=1
        print(chunk_no)
        future = executor.submit(find_common_triples, chunk, fb5m_data, common_data,chunk_no)
        futures.append(future)
    # Wait for all threads to complete
    concurrent.futures.wait(futures)

# common_data now contains the common triples

# Use threading to find common triples in parallel
len(common_data)

output_file = "FB15k-237-Output/common_train.txt"
with open(output_file, "w", encoding="utf-8") as outputf:
    for triple in common_data:
        outputf.write(f"{triple[0]}\t{triple[1]}\t{triple[2]}\t{triple[3]}\n")
