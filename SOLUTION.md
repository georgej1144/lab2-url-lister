# LAB 2
## URLCount1.java

To modify WordCount1.java into URLCount1.java, I changed the Mapper function to match the URL by regex instead of matching every word. With only URLs being passed to the reducers, I only had to change their argument types from IntIterable to LongIterable for RegexMapper to work properly.

## Distributed Running

To launch the gcloud cluster with 2 workers I had to modify the command to the following
```
gcloud dataproc clusters create test-dataproc \
    --project=csci4253-lab2 \
    --region=us-east4 \
    --zone=us-east4-a \
    --master-machine-type=e2-standard-2 \
    --worker-machine-type=e2-standard-2 \
    --master-boot-disk-size=50GB \
    --worker-boot-disk-size=50GB \
    --num-workers=2 \
    --public-ip-address
```
where I changed the project ID and specified disk sizes because the default allocations overran the 2TB student limit.

I accessed the master box using `gcloud compute ssh test-dataproc-m` and cloned the repo into the server `git clone https://github.com/georgej1144/lab2-url-lister.git`. Enter the repository `cd lab2-url-lister`

From here the filesystem can be pepared with `make filesystem`, the data can be downloaded with `make prepare`, the code can be built with `make`. Now `make run` will execute the workload on the worker machines and `time make run` will do the same but time it. 

On distributed systems, the results won't be returned in 1 file. Display the full result with `hdfs dfs -cat output/part-r-*`



Running the workload on 2 nodes was significantly slower than running locally. Presumably, this is due to the intense overhead of setting up the two machines to do a very easy task. On 4 nodes the time only gets slower. This is because there is **even more** overhead, but the workload of 2 files cannot be shared any further between 4 systems as it can between 2 systems.




## Cleanup

Gcloud cleanup of the clusters can be done with the following `gcloud dataproc clusters delete [cluster name] --region=[region]`.


## Collaboration

This assignment was worked on exclusively by me, George Johnson.
claude.ai used to annotate the original WordCount1.java provide guidance on `gcloud` operation, and 