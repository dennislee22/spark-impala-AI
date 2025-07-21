# Spark -> Hive/Impala -> AI/ML
![dataservice-flow](https://github.com/user-attachments/assets/491d8816-f8ac-41e6-a855-568c4b9c1ec4)

There's a fundamental reason why Cloudera Data Services is engineered to consolidate CDE, CDW, and CAI on a unified platform, powered by K8s technology. This integrated approach delivers a seamless and secure experience across the entire data lifecycle:
- CDE (Cloudera Data Engineering): Leverages Apache Spark and Airflow for robust and automated data transformation pipelines, ensuring data is prepared efficiently for downstream use.
- CDW (Cloudera Data Warehouse): Provides powerful SQL data analytics capabilities, leveraging Hive/Impala SQL engines for high-performance business intelligence and reporting.
- CAI (Cloudera AI): Empowers data scientists with ready-to-use inferencing service (engineering with Nvidia NIM) and JupyterLab IDE for exploratory data analysis. CAI enables rapid insights and model development directly on transformed data, all while maintaining stringent data security within the data lake.

This article illustrates a simple end-to-end data lifecycle use case leveraging CDE, CDW and CAI, all under one roof. The aim is to transform the raw data sitting in the data lake into the usable dataframe for analysis purposes at a later stage.

## <a name="toc_0"></a>Table of Contents
[//]: # (TOC)
1. [Run PySpark Job in CDE to transform CSV into Hive/Impala Table](#toc_0)<br>
2. [Verify Hive/Impala table in CDW](#toc_1)<br>
3. [EDA with Cloudera AI (CAI) Workbench](#toc_2)<br>

![dataservices-flow2](https://github.com/user-attachments/assets/259dc563-dbc0-4b9f-8052-c2d821d77eb5)

### <a name="toc_0"></a>1. Run PySpark Job in CDE to transform CSV into Hive/Impala Table

1. The content of the raw data `data.csv` residing in the HDFS is shown as follows. 

```
id,name,value,city
1,Charlie,192,Singapore
2,Heidi,79,Tokyo
3,Judy,101,New York
4,Heidi,451,London
5,David,430,Berlin
6,Bob,350,Berlin
.....
```
   
2. Create a Spark job in CDE by simply uploading this [csv-hive](csv-hive.py) or [csv-impala](csv-impala.py) script. This script will create a Spark session to transform data in CSV into a Hive/Impala table.
<img width="800" alt="image" src="https://github.com/user-attachments/assets/5533e598-e338-4b29-b865-ad04805189d9" />


3. Spark job needs to know the source of Hive Metasore server. Enter the following Spark configurations in the job.
```
spark.sql.hive.hiveserver2.jdbc.url=jdbc:hive2://base-01.dlee5.cldr.example:10000/default
spark.sql.hive.hiveserver2.jdbc.url.principal=hive/_HOST@CLDR.EXAMPLE
```

4. Upon successful execution of the job, you will see the similar snippet of the Spark driver log as follows.
<img width="800" alt="image" src="https://github.com/user-attachments/assets/8cc15600-e658-4415-8115-aa77f6822ed3" />

```
Spark Session created with Hive support.
Successfully read CSV from: hdfs:///user/dennis/data.csv
Original DataFrame Schema:
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- value: integer (nullable = true)
 |-- city: string (nullable = true)

Original DataFrame Sample:
+---+-------+-----+---------+
| id|   name|value|     city|
+---+-------+-----+---------+
|  1|    Eve|  393|      Rio|
|  2|Charlie|  405|   Sydney|
|  3|  Frank|  277| New York|
|  4|  David|  293|Singapore|
|  5|   Judy|  319|    Dubai|
+---+-------+-----+---------+
only showing top 5 rows


Transformed DataFrame Schema:
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- amount: integer (nullable = true)
 |-- city: string (nullable = true)
 |-- processed_date: string (nullable = false)

Transformed DataFrame Sample:
+---+-------+------+---------+--------------+
| id|   name|amount|     city|processed_date|
+---+-------+------+---------+--------------+
|  1|    Eve|   393|      Rio|    2025-06-25|
|  2|Charlie|   405|   Sydney|    2025-06-25|
|  3|  Frank|   277| New York|    2025-06-25|
|  4|  David|   293|Singapore|    2025-06-25|
|  5|   Judy|   319|    Dubai|    2025-06-25|
+---+-------+------+---------+--------------+
only showing top 5 rows
```

5. As a result, the system creates the parquet files in the designated tablespace of the database, sitting in the datalake.
   
<img width="800" alt="image" src="https://github.com/user-attachments/assets/ae45c7a9-3169-4b4b-8f60-536d002bb316" />

### <a name="toc_1"></a>2. Verify Hive/Impala table in CDW

1. You may use Hue dashboard via CDW to verify the location of the output.
<img width="800" alt="image" src="https://github.com/user-attachments/assets/e252ca37-02b2-4593-8cc6-c797f1d57173" />

2. Run simply SQL query to verify the table content.
<img width="800" alt="image" src="https://github.com/user-attachments/assets/d1d3d149-ddd7-4b61-b104-e2b6da0aa1af" />

### <a name="toc_2"></a>3. EDA with Cloudera AI (CAI) Workbench

1. Create a Jupyterlab session in CAI with Spark enabled.
<img width="800" alt="image" src="https://github.com/user-attachments/assets/e8cc5605-735e-4eff-9538-e14baf9ede15" />

2. Besides CDE, I can also run Spark job (as illustrated in [run-EDA.ipynb](run-EDA.ipynb)) inside CAI Workbench to handle data transformation. In this case, I create a Spark Session with Hive support to read Impala table (sitting in the data lake) and subsequently converting `Spark DataFrame` into `Pandas DataFrame`. During this process, the system spawns Spark executor pods in the underlying K8s platform to carry out the Spark job accordingly.
```
NAME                            READY   STATUS    RESTARTS   AGE
43fbd1dyokdze92c                5/5     Running   0          114s
cdsw-43fbd1dyokdze92c-exec-1    5/5     Running   0          32s
cdsw-43fbd1dyokdze92c-exec-2    5/5     Running   0          12s
```
3. Alternatively, I can alsoo run Spark job in the YARN cluster in the data lake as illustrated below. Simply enable the `Spark pushdown` option in the CAI project and subsequently create a new CAI session, run the same [EDA script](run-EDA.ipynb).
<img width="750" alt="image" src="https://github.com/user-attachments/assets/6a59eb0c-371a-4d50-b98b-8fbbee96d537" />
<img width="800" alt="image" src="https://github.com/user-attachments/assets/d60744d5-8fe2-41b1-a5ce-ac74477ce005" />

4. Finally, use `matplotlib.pyplot` Python library to produce visual diagrams based on the converted `Pandas DataFrame`.
<img width="800" alt="image" src="https://github.com/user-attachments/assets/66cc6752-65d4-4fba-b86b-339c98c15523" />

### Conclusion
Cloudera Data Services unifies Spark/Airflow for ETL, Hive/Impala for SQL analytics, and JupyterLab/Spark for EDA and AI model development. on a single Kubernetes platform for a complete data lifecycle. This integrated approach ensures data is governed and secured in one place across all analytics and AI functions. Ultimately, this removes the "integration tax", eliminating the cost and complexity of connecting disparate third-party systems.







