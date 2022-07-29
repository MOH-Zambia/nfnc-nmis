import json
import pathlib

import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from csv import reader

dag = DAG(
    dag_id="hmis-nmis-pipeline",
    description="Import nutriton data from Ministry of Health HMIS DHIS2 instance to National NFNC NMIS DHIS2 instance",
    start_date=dt.datetime(year=2019, month=1, day=1),
    schedule_interval="@monthly",
)

download_hmis_organisation_units = BashOperator(
    task_id="download_hmis_organisation_units",
    bash_command="curl -o /tmp/hmis_organisation_units.csv -L 'https://dhis2.moh.gov.zm/dev/api/29/sqlViews/K05RbqAt36s/data.csv?skipPaging=true'",  # noqa: E501
    dag=dag,
)


def _get_data_values():
    # Ensure directory exists
    pathlib.Path("/tmp/data/{{ds}}").mkdir(parents=True, exist_ok=True)

    startDate=airflow.utils.dates.months_ago(3)
    endDate={{ds}}
    # endDate={{next_ds}}

    # open file in read mode
    with open('/tmp/hmis_organisation_units.csv', 'r') as read_obj:
        # pass the file object to reader() to get the reader object
        csv_reader = reader(read_obj)
        # Iterate over each row in the csv using reader object
        for row in csv_reader:
            # row variable is a list that represents a row in csv
            print(row)
            uri = 'https://dhis2.moh.gov.zm/dev/api/29/dataValueSets?dataSet=comn89vr6oX&orgUnit=%s&startDate=%s&endDate=%s' % (row[0], startDate, endDate)
            response = requests.get(uri, auth=('NFNC', 'Nfnc@2022'))
            data = response.json()
            with open(row[0] + '_{{ds}}.json', 'w') as f:
                json.dump(data, f)


get_data_values = PythonOperator(
    task_id="data_values", python_callable=_get_data_values, dag=dag
)



notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/data/ | wc -l) data files."',
    dag=dag,
)

download_hmis_organisation_units >> get_data_values >> notify
