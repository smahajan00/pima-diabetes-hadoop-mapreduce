# Hadoop MapReduce – Diabetes Data Analysis

This repository contains the Java MapReduce source code and dataset used for the
mini project "Analysis of Diabetes Patient Records Using Hadoop MapReduce"
for the module COMP30037 – Programming for Big Data.

## Dataset
Pima Indians Diabetes Dataset (Kaggle)

## Files
- pmdMapper.java – Mapper implementation
- pmdReducer.java – Reducer implementation
- pmdDriver.java – Driver class
- diabetes.csv – Input dataset

## Execution
1. Upload diabetes.csv to HDFS
2. Compile and export JAR using Eclipse
3. Run MapReduce job using:
   hadoop jar pima.jar pmdDriver /pima_diabetes /results
