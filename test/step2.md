Switch to the _IDE tab_ 

In the file list you will see a folder `airflow`. Open `airflow.cfg` for editing

We need to tell Airflow where to find our dag definition. Change the following line to point to `/root/dags`:
`dags_folder = /root/dags`

To the right of the _IDE Tab_ you will see a **+** sign. Click this and select "Open New Terminal"

Verify that the dag code executes without errors:
`python dags/dag.py`{{execute}} 

Now that Airflow knows where the dag files are the `data_pipeline_demo` will be present in the dag list:

`airflow dags list | grep data_pipeline_demo`{{execute}} 

You also need to reinitialize the db to include the new dag:
`airflow db init`{{execute}}

<!-- Run scheduler.
`airflow scheduler`{{execute}} -->

If you go back to the [Airflow UI](https://[[HOST_SUBDOMAIN]]-8080-[[KATACODA_HOST]].environments.katacoda.com/) and reload, you should now see "data_pipeline_demo" listed at the top of the dag list. Toggle the button next to it to unpause the dag.

At this point you can run the pipeline by launching the dag:
`airflow dags trigger data_pipeline_demo`{{execute}}

You'll also need to start the scheduler so the tasks will be scheduled:
`airflow scheduler`{{execute}}
