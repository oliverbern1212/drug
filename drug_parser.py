from airflow.operators import BashOperator, PythonOperator
from airflow.models import DAG
from datetime import datetime
from urlparse import urlparse
from os.path import splitext, basename
import subprocess
import zipfile
import os.path
import logging

args = {
    'start_date': datetime.now(),
    'owner': 'airflow',
}

#Logger
logger = logging.getLogger()

dag = DAG(
    dag_id='drug_parser',
    default_args=args,
    schedule_interval=None)




# Validation whether a zip file is corrupt
def validation(filename):

    #Check whether a file exsit in folder.
    if os.path.exists(filename) == False:
        return True

    try:
        file = zipfile.ZipFile(filename)
        ret = file.testzip()

        #True: Normal file.
        #False: Corrupt file.
        if ret is not None:
            os.remove(filename)
            return True

        return False
    except:
        logger.info('{} is corrupted.Try to download again!'.format(filename))
        os.remove(filename)
        return True

def download_func(ds, **kwargs):
    temp_dir = kwargs['dag_run'].conf['temp_dir']
    base_url = kwargs['dag_run'].conf['base_url']
    url      = kwargs['dag_run'].conf['url']

    disassembled = urlparse(url)
    filename, file_ext = splitext(basename(disassembled.path))

    if (validation(temp_dir+ filename + file_ext)):
        bashcmd = "wget " + base_url + url + " -P " + temp_dir

        #Downloading a bash command to download a zip file.
        print("Downloading: {} ".format(filename + file_ext, temp_dir))

        logger.info("{} downloading is started!".format(filename + file_ext))
        output = subprocess.check_output(['bash', '-c', bashcmd])
        logger.info("{} downloading is finished!".format(filename + file_ext))

    #Storing values to use in parser task.
    kwargs['ti'].xcom_push(key='filename', value=filename + file_ext)
    kwargs['ti'].xcom_push(key='temp_dir', value=temp_dir)


def parse_func(ds, **kwargs):

    ti = kwargs['ti']
    filename = ti.xcom_pull(key='filename', task_ids='download')
    temp_dir = ti.xcom_pull(key='temp_dir', task_ids='download')


    print( "Parsing {} is started!".format(filename))
    logger.info("{} parsing is started!".format(filename ))

    #Running drug_reaction_processor.py file.
    bashcmd = "python drug-reaction-processor.py " + temp_dir + filename
    subprocess.check_output(['bash', '-c', bashcmd])
    logger.info("{} parsing is finished!".format(filename ))

def subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
      dag_id='%s.%s' % (parent_dag_name, child_dag_name),
      default_args=args,
      schedule_interval=None,
    )

    #task to download.
    task_download = PythonOperator(
        task_id='download',
        depends_on_past=False,
        provide_context=True,
        python_callable=download_func,
        dag=dag)

    #task to parse.
    task_parse = PythonOperator(
        task_id="parse",
        depends_on_past=False,
        provide_context=True,
        python_callable=parse_func,
        dag=dag)

    #Setting an upstream.
    task_parse.set_upstream(task_download)

    return dag_subdag



