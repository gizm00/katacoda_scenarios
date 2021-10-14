## Running the DAG
Return to the second terminal window you opened in step 2.  

At this point you can run the pipeline by launching the dag:
`airflow dags trigger data_pipeline_demo`{{execute}}

You'll also need to start the scheduler so the tasks will be scheduled:
`airflow scheduler`{{execute}}  

Navigate back to the Airflow UI and reload the page. In the `Graph View` you can watch each task execute. You can mouseover each step to see information  
