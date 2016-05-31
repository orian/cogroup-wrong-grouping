# Items not groupped correctly - CoGroupByKey - FlinkPipelienRunner
 
The test uses a similar data structure as my initial data.

There are two data PCollections. One has to run CreateData first to generate data files.

Afterwards the CoGroupPipeline could be run.

I use:
 - bleeding edge [apache/incubating-beam](https://github.com/apache/incubator-beam)
 - maven 3.3.3
 - Oracle Java 1.8
 
**Run**

Data generator

    mvn exec:java -Dexec.mainClass="eu.pawelsz.apache.beam.CoGroupPipelien"

Pipeline

    mvn exec:java -Dexec.mainClass="eu.pawelsz.apache.beam.CoGroupPipelien" -Dexec.args="--parallelism=6"
       
Notice: we specify to use a parallelism.
    
What to look for?

In execution log one can find a keys for which there were missing data1 value:
 
    41730 data2 items for (hereGoesLongStringID1,3) marked as no-d1
    
You can spot, that many keys were processed more than once:

    41510 data2 items for (hereGoesLongStringID0,2)
    ...
    41730 data2 items for (hereGoesLongStringID0,2) marked as no-d1

The summary will print how many key values missed a data1.

    INFO [eu.pawelsz.apache.beam.CoGroupPipeline.main()] (FlinkPipelineRunner.java:127) - data2 count : 0
    INFO [eu.pawelsz.apache.beam.CoGroupPipeline.main()] (FlinkPipelineRunner.java:127) - item count : 3750000
    INFO [eu.pawelsz.apache.beam.CoGroupPipeline.main()] (FlinkPipelineRunner.java:127) - missing data1 : 45

## Workarounds

As for now I have two workarounds and ignorance:

 1. If there is one dominant dataset and other datasets are small (size << GB) then I use SideInput. **OK - verified**
 2. If I have multiple datasets of similar size I enclose it in a common container, flatten it and GroupByKey. **FAIL - verified**
 3. I measure occurrences and ignore the bug for now. **SUCKS**

To verify workaround 2, set the `CoGroupPipiline.MODE=2`.