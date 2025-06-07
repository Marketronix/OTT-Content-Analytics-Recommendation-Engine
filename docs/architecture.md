<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>OTT Content Analytics Architecture Documentation</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            padding: 20px;
        }
        h1, h2 {
            color: #2c3e50;
        }
        .diagram {
            border: 1px solid #ccc;
            padding: 20px;
            margin-bottom: 20px;
            background-color:rgb(26, 177, 51);
            font-family: monospace;
            white-space: pre;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 20px;
        }
        th, td {
            border: 1px solid #999;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color:rgb(253, 171, 8);
        }
        code {
            background-color: #f4f4f4;
            padding: 2px 4px;
            font-family: monospace;
        }
    </style>
</head>
<body>

<h1>OTT Content Analytics Architecture Documentation</h1>

<h2>1️⃣ High-Level Data Flow</h2>
<div class="diagram">
+-------------+          +---------------------------+
|   GitHub    |  Push    |  GCS Bucket: composer-dags |
+-------------+  ---->   +---------------------------+
                              |
                              V
                      +----------------+
                      | Cloud Composer  |
                      | (Airflow)       |
                      +----------------+
                              |
                              |
                              V
+---------------+     +----------------+     +--------------------+
| Local Machine | --> | GCS Raw Data    | --> | BigQuery / Processed |
| download_imdb_data.py | ott-analytics-raw-data | ott-analytics-processed-data |
+---------------+     +----------------+     +--------------------+
</div>

<h2>2️⃣ Service Interactions</h2>
<table>
    <thead>
        <tr>
            <th>Component</th>
            <th>Interaction Purpose</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>GitHub</td>
            <td>Stores DAGs and triggers deploy to Composer GCS bucket</td>
        </tr>
        <tr>
            <td>GitHub Actions</td>
            <td>Deploys DAGs to Composer via <code>.github/workflows/deploy-dags.yml</code></td>
        </tr>
        <tr>
            <td>Cloud Composer (Airflow)</td>
            <td>Orchestrates DAGs and manages pipeline scheduling</td>
        </tr>
        <tr>
            <td>Local Machine</td>
            <td>Runs initial data download script</td>
        </tr>
        <tr>
            <td>IMDb Dataset</td>
            <td>Source of raw data (.tsv.gz files)</td>
        </tr>
        <tr>
            <td>GCS Raw Data Bucket</td>
            <td>Stores raw IMDb data</td>
        </tr>
        <tr>
            <td>GCS Processed Data Bucket</td>
            <td>Stores cleaned/transformed data</td>
        </tr>
        <tr>
            <td>BigQuery</td>
            <td>Stores final analytics-ready tables</td>
        </tr>
    </tbody>
</table>

<h2>3️⃣ DAG Dependencies (Current Phase)</h2>
<table>
    <thead>
        <tr>
            <th>Task</th>
            <th>Description</th>
            <th>Dependency</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Sensor (<code>GCSObjectsWithPrefixExistenceSensor</code>)</td>
            <td>Wait for files in raw GCS bucket</td>
            <td>Independent start</td>
        </tr>
        <tr>
            <td><code>LocalFilesystemToGCSOperator</code></td>
            <td>Upload local IMDb data to GCS raw bucket</td>
            <td>After manual download</td>
        </tr>
        <tr>
            <td>Future: Transformation DAG</td>
            <td>Read raw → transform → write processed GCS / BigQuery</td>
            <td>After ingestion DAG</td>
        </tr>
    </tbody>
</table>

<h2>4️⃣ Future Improvements — Suggested DAG Flow</h2>
<div class="diagram">
[ download_imdb_data (PythonOperator) ]
                |
                V
[ Upload to GCS (LocalFilesystemToGCSOperator) ]
                |
                V
[ Validate Files (Sensor) ]
                |
                V
[ Data Transformation (Dataflow / Spark / PythonOperator) ]
                |
                V
[ Load to BigQuery ]
</div>

<h2>Summary Table</h2>
<table>
    <thead>
        <tr>
            <th>Layer</th>
            <th>Technology</th>
            <th>Role</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Orchestration</td>
            <td>Cloud Composer (Airflow)</td>
            <td>DAG orchestration</td>
        </tr>
        <tr>
            <td>Ingestion</td>
            <td>Python script + Airflow DAG</td>
            <td>IMDb → GCS ingestion</td>
        </tr>
        <tr>
            <td>Storage (raw)</td>
            <td>GCS: ott-analytics-raw-data</td>
            <td>Store raw IMDb files</td>
        </tr>
        <tr>
            <td>Processing</td>
            <td>Airflow tasks or Dataflow</td>
            <td>Transform data</td>
        </tr>
        <tr>
            <td>Storage (processed)</td>
            <td>GCS: ott-analytics-processed-data</td>
            <td>Store cleaned data</td>
        </tr>
        <tr>
            <td>Analytics</td>
            <td>BigQuery: ott_analytics</td>
            <td>Query-ready dataset</td>
        </tr>
    </tbody>
</table>

</body>
</html>
