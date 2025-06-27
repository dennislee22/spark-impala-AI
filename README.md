# Spark -> Hive/Impala -> ML/AI
<img width="500" src="https://github.com/user-attachments/assets/351d84a4-88ff-45b5-bd19-b4efcdca2cbf" />


## Create Spark Job in CDE

<img width="1436" alt="image" src="https://github.com/user-attachments/assets/5533e598-e338-4b29-b865-ad04805189d9" />

```
spark.sql.hive.hiveserver2.jdbc.url=jdbc:hive2://base-01.dlee5.cldr.example:10000/default
spark.sql.hive.hiveserver2.jdbc.url.principal=hive/_HOST@CLDR.EXAMPLE
```

<img width="1444" alt="image" src="https://github.com/user-attachments/assets/8cc15600-e658-4415-8115-aa77f6822ed3" />

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

## Run PySpark Job in CDE to transform CSV into Hive Table

<img width="1448" alt="image" src="https://github.com/user-attachments/assets/ae45c7a9-3169-4b4b-8f60-536d002bb316" />

## Verify Hive/Impala table in CDW
<img width="1450" alt="image" src="https://github.com/user-attachments/assets/e252ca37-02b2-4593-8cc6-c797f1d57173" />

<img width="1458" alt="image" src="https://github.com/user-attachments/assets/d1d3d149-ddd7-4b61-b104-e2b6da0aa1af" />


## EDA with Cloudera AI (CAI) Workbench

<img width="1443" alt="image" src="https://github.com/user-attachments/assets/e8cc5605-735e-4eff-9538-e14baf9ede15" />

- I can also run Spark job inside CAI Workbench to handle data transformation. In this case, I create a Spark Session with Hive support for reading Impala table (sitting in the data lake) and subsequently converting `Spark DataFrame` to `Pandas DataFrame`. During this process, the system spawns Spark executor pods in the underlying K8s platform to carry out the Spark job seamlessly.
```
NAME                            READY   STATUS    RESTARTS   AGE
43fbd1dyokdze92c                5/5     Running   0          114s
cdsw-43fbd1dyokdze92c-exec-1    5/5     Running   0          32s
cdsw-43fbd1dyokdze92c-exec-2    5/5     Running   0          12s
```
- Alternatively, you may also run Spark job in the YARN cluster in the data lake if you are running out of resources in the K8s platform.
<img width="750" alt="image" src="https://github.com/user-attachments/assets/6a59eb0c-371a-4d50-b98b-8fbbee96d537" />
<img width="1449" alt="image" src="https://github.com/user-attachments/assets/d60744d5-8fe2-41b1-a5ce-ac74477ce005" />

- Finally, use matplotlib.pyplot to produce visual diagrams based on the converted `Pandas DataFrame`. Check out the entire EDA script.
<img width="1428" alt="image" src="https://github.com/user-attachments/assets/66cc6752-65d4-4fba-b86b-339c98c15523" />









