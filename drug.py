from airflow import DAG, utils
from airflow.operators import *
from datetime import date, datetime, time, timedelta

from urlparse import urlparse
from os.path import splitext, basename
from bs4 import BeautifulSoup
import urllib2
import zipfile
import os.path
import logging

#page url to download zip files.
page_url = 'http://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm082193.htm'
base_url = "http://www.fda.gov"
temp_dir = "/tmp/"

#Logger
logger = logging.getLogger()


# This function decides whether or not to Trigger the remote DAG
def controller_trigger(context, dag_run_obj):

    logger.info("Drug controller started!")
    print("Drug controller started!")

    soup = BeautifulSoup(urllib2.urlopen(page_url).read())

    #scrap zip file names.
    list_contents = soup('div', {'class': 'panel-body'})[0]
    lists = list_contents.find_all('li')

    #Add file names to list value.
    for li in lists:
        url = li.find_all('a')[0].get('href')

        #extract file name from url.
        disassembled = urlparse(url)
        filename, file_ext = splitext(basename(disassembled.path))

        dag_run_obj.payload = {'url' :url,
                               'temp_dir': temp_dir,
                               'base_url': base_url
                              }
        return dag_run_obj

# Define the DAG
dag = DAG(dag_id='drug',
          default_args={"owner" : "me",
          "start_date":datetime.now()},
          schedule_interval='@once')


# Define the single task in this controller DAG
trigger = TriggerDagRunOperator(task_id='drug_controller',
                                trigger_dag_id="drug_parser",
                                python_callable=controller_trigger,
                                dag=dag)