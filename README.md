# TPGenerator: Lauca

- The code in TPGenerator provides the component of Transaction Logic Analyzer, Data Access Distribution Analyzer, Performance Monitor, Data Characteristics Extractor, Workload Generator and Database Generator in [code](code/). 
- Technique Report can be found in [technical report](technical-report.pdf).

## Quick Start for TiDB with PingCAP

- We can simply use the following commands to simulate data and workloads.  The TiDB example in [LaucaExample](LaucaExample). Here, we provides a configuration file about TiDB in both [LaucaProduction](./LaucaExample/Production/lauca-tidb.config) and [LaucaTesting](./LaucaExample/Testing/lauca-tidb.conf)，and according jar files in [LaucaProduction.jar](./LaucaExample/Prodution/LaucaProduction.jar)  and [LaucaTesting.jar](./LaucaExample/Testing/LaucaTesting.jar).

```shell

cd LaucaExample
cd Production
# this step will automatically catpured the data characteristics and then create dataCharacteristicSaveFile.obj in testdata file
java -jar LaucaProduction.jar ./lauca-tidb.conf --getDataCharacteristics
# this step will automatically catpured the workload characteristics(transaction logic and data access distribution) and then create txLogicSaveFile.obj  and distributionSaveFile.obj in testdata file
java -jar LaucaProduction.jar ./lauca-tidb.conf --getWorkloadCharacteristics

#open anothor terminal
cd LaucaExample
cd Testing
# this step will automatically generate data in testdatab/data according to the dataCharacteristicSaveFile.obj
java -jar LaucaTesting.jar ./lauca-tidb.conf --geneSyntheticDatabase
# this step will automatically load data from testdatab/data into TiDB database which is configurated in lauca-tidb.conf.
java -jar LaucaTesting.jar ./lauca-tidb.conf --loadSyntheticDatabase
# this step will automatically generate workload into the TiDB database according dataCharacteristicSaveFile.obj, txLogicSaveFile.obj  and distributionSaveFile.obj.
java -jar LaucaTesting.jar ./lauca-tidb.conf --geneSyntheticWorkload
```



## Example to get workload trace

- We can get the workload trace via skywalking, the link is https://github.com/apache/skywalking
- The application can print workload trace through java agent. Here we use oltp-bench which generates TPC-C as our application side. User can use the following step to load database and print workload trace.

```
cd oltpbench
# configurate the database info
vim config/tpcc_config_tidb.xml
# load the database
./oltpbenchmark -b tpcc -c config/tpcc_config_tidb.xml --create=true --load=true
# generate the workload into databases and print the workload trace
./oltpbenchmarkSkywalking -b tpcc -c config/tpcc_config_tidb.xml --execute=true
```

- But we also provide a workload trace about TPC-C with 20 warehouses, 20 connections and 100 second running time available in [workload trace](./lauca-log)



## Overview of Program Files

- In the *LaucaExample* folder, there are three sub folders, namely, the production environment, the testing environment and testdata. In the production environment, the basic data feature *dataCharacteristicSaveFile.obj*, the transaction logic feature *txLogicSaveFile.obj* and the workload access distribution feature *distributionSaveFile.obj* are generated in *testdata*. The testing environment generates a synthetic database using the basic data features,  which create *tables* file  in *testdata* and then used to generate database through its second step. In addition, the testing environment generates simulated workloads using transaction logic and workload access distribution.  *LaucaProduction.jar*  is in the Production folder and *LaucaTesting.jar* is in the Testing  folder. The former one is responsible for extracting the data features and workloads features, while the latter oen is responsible for generating the simulated database and simulated workloads.

```tree
LaucaExample/
├── Production
│   ├── lauca-tidb.conf
│   ├── LaucaProduction.jar
│   └── log4j.properties
├── testdata
│   ├── dataCharacteristicSaveFile.obj
│   ├── distributionSaveFile.obj
│   ├── tables
│   └── txLogicSaveFile.obj
└── Testing
    ├── lauca-tidb.conf
    ├── LaucaTesting.jar
    └── log4j.properties

```

## Supplementary Experiments

### Experiment Setting

* We conduct our experiments on a sever equipped with 48 Intel(R) Xeon(R) Gold 6126 @ 2.60Ghz CPUs, 250GB memory, 8TB disk.   
* We deploy both client and database service on the same machine.
* We use MySQL v5.7.27 and TiDB v5.0.0.

### Experiment for Review1-O2
* In order to illustrate that our simulation is also good if the production environment and the evaluation environment use different databases, we run a new set of experiment. 
* Supposing we load TPC-C workload into MySQL, Lauca extracts the characteristcis of both data and workload to generate a simulated scenario (data and workload). The simulated scenario and the real scenario (TPC-C workload from OLTP-Bench) are loaded into TiDB, respectively. We then compare their performance on TiDB. 
* Figure 1 shows performance deviations between simulated workloads generated by Lauca and real workloads generated by OLTP-Bench, under 10 warehouses and 10 threads. We can see that these two workloads are very similar in throughput and average latency, where the deviations are 2.94% and 6.65% respectively.  

<p align="center">
  <img src="https://github.com/luyiqu/Lauca/blob/main/img/Figure1.png?raw=true" alt="Figure 1",width="300" height="200"/>
</p>
<p align="center">Figure 1</p>
### Experiment for Review2-O6

* We deploy the client and server on the same machine through the isolation of NUMA node. 
* We do experiments respectively through local loopback and unix domain socket to avoid the slow network.
* In this experiment, we run TPC-C workload on MySQL (v5.7.27), using a database of 20 warehouses.
* Figure 2(a)-(c) shows performance deviations between simulated workloads generated by Lauca and real workloads generated by OLTP-Bench. 
<figure class="third"> 
    <img src="https://github.com/luyiqu/Lauca/blob/main/img/Figure%202(a)%20Throughput.png?raw=true" alt="Figure 2(a)" title="Figure 2(a) Throughput" width="300" /><img src="https://github.com/luyiqu/Lauca/blob/main/img/Figure%202(a)%20Throughput.png?raw=true" alt="Figure 2(b)" title="Figure 2(b) Avg Latency" width="300"/><img src="https://github.com/luyiqu/Lauca/blob/main/img/Figure%202(c)%2095%20latency.png?raw=true" alt="Figure 2(c)" title="Figure 2(c) 95% Latency" width="300"/> </figure> 

​		Figure 2(a) Throughput						            	Figure 2(b) Avg Latency								Figure 2(c) 95% Latency	



## Example for calculate probability of a dependency relationship (Review2-O1(3))
- Here we take an equal relationship (ER) calculation as an example. 
- Suppose TXN<sub>1</sub> has two statements:
    > update S set s<sub>3</sub> = s<sub>3</sub> + p<sub>1,1</sub> where s<sub>1</sub> = p<sub>1,2</sub>;
    > update T set t<sub>3</sub> = t<sub>3</sub> + p<sub>2,1</sub> where t<sub>1</sub> = p<sub>2,2</sub> and t<sub>2</sub> = p<sub>2,3</sub>;
- If the total number of transaction instances for TXN<sub>1</sub> is T. When calculating ER probability for p<sub>2,3</sub> , we compare it with the values from each previous statement, i.e., p<sub>1,1</sub>, p<sub>1,2</sub>, p<sub>2,1</sub> and p<sub>2,2</sub>. If p<sub>1,2</sub> = p<sub>2,3</sub> in t instances, we have ER for p<sub>2,3</sub>, i..e, [p<sub>1,2</sub>, p<sub>2,3</sub>, ER, t/T]. 
