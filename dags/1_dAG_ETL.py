from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
#import required modules
import requests, json
import os
from datetime import datetime, timedelta


default_args = {
    'owner': 'Asad Ali',
}
dag = DAG(
    'A_ETL', start_date=datetime(2024, 6, 10),default_args=default_args,schedule_interval=timedelta(minutes=1), catchup=False
)
def Extract_Cur(ti): 
    data = []
    # Give city name
    city_name = "Karachi"   
    # Enter your API key here
    api_key = "9fcb3f002a5b7302ee47017a37e0a031"
    # base_url variable to store url
    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    # complete_url variable to store
    # complete url address
    complete_url = base_url + "appid=" + api_key + "&q=" + city_name

    # get method of requests module
    # return response object
    response = requests.get(complete_url)

    # json method of response object
    # convert json format data into
    # python format data
    x = response.json()
    
    # Now x contains list of nested dictionaries
    # Check the value of "cod" key is equal to
    # "404", means city is found otherwise,
    # city is not found
    if x["cod"] != "404":
        # store the value of "main"
        # key in variable y
        y = x["main"]
        current_temperature = y["temp"]
        current_pressure = y["pressure"]
        current_humidity = y["humidity"]
        z = x["weather"]
        weather_description = z[0]["description"]
    else:
        current_temperature,current_pressure,current_humidity,weather_description=0
    data=[current_temperature,current_pressure,current_humidity,weather_description]
    ti.xcom_push(key='curr_data', value=data)
    #return(data) 

def Extraxt_Pred(ti):
    city = "Karachi"   
    data = []
    current_time = datetime.now()
    one_hour_later = current_time + timedelta(hours=1)
    unix_timestamp = int(one_hour_later.timestamp())
    
    api_key = "9fcb3f002a5b7302ee47017a37e0a031"
    url = f'http://api.openweathermap.org/data/2.5/forecast?q={city}&dt={unix_timestamp}&appid={api_key}'
    response = requests.get(url)
    x = response.json()
    
    # Now x contains list of nested dictionaries
    # Check the value of "cod" key is equal to
    # "404", means city is found otherwise,
    # city is not found
    if x["cod"] != "404":

        # store the value of "main"
        # key in variable y
        y = x["list"][0]['main']

        # Transform data with Pandas
        #converting Kelvint to Cel
        current_temperature = y["temp"]
        current_pressure = y["pressure"]
        current_humidity = y["humidity"]
        weather_description=x["list"][0]['weather'][0]["description"]
    else:
        current_temperature,current_pressure,current_humidity,weather_description=0
        
    data=[current_temperature,current_pressure,current_humidity,weather_description]
    ti.xcom_push(key='pred_data', value=data)


#transform temp in Celcis and tranform data into key value pair
def transform(ti):
    data = ti.xcom_pull(key='curr_data', task_ids='current')
    current_dateTime=datetime.now()
    # Data to be written
    transformed_data = {
        
        "Date":current_dateTime.year,
        "Timee":current_dateTime.hour+current_dateTime.minute,
        "current_temperature": data[0]-273.15,
        "current_pressure": data[1],
        "current_pressure": data[2],
        "weather_description": data[3]
    }
    ti.xcom_push(key='trans_data', value=transformed_data)
 
def load(ti):
    # Serializing json
    transformed_data = ti.xcom_pull(key='curr_data', task_ids='current')
    ti.xcom_push(key='load', value=transformed_data)
    
    json_object = json.dumps(transformed_data, indent=5)
    # Writing to sample.json D:\\ALLINONE\\Desktop\\docker
    directory_path = r'D://docker//airflow//'
    os.makedirs(directory_path, exist_ok=True)
    file_path = os.path.join(directory_path, 'sample.json')
    with open(file_path, "a") as outfile:
        outfile.write(json_object)

extract = PythonOperator(
    task_id='current',
    python_callable=Extract_Cur,
    dag=dag
)

tranform = PythonOperator(
    task_id='tranform',
    python_callable=transform,
    dag=dag
)

load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag
)
extract >> tranform >> load