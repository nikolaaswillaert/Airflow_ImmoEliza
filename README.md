# Airflow pipeline for ImmoEliza  :house_with_garden:


## :information_source: Information

This airflow pipeline uses the code from the previous Immo Eliza Project (https://github.com/nikolaaswillaert/ImmoEliza) and runs a number of tasks, including **scraping the data** from the website, **cleaning** the data, **uploading** the cleaned dataset to a **S3 bucket** (minio or AWS), **train an xgBoost model** on the latest cleaned dataset and finally uploads the most recent model to a different S3 bucket (minio or AWS) so it can be used to make a prediction (in the streamlit app)

The pipeline will **execute every day at 8pm**. This variable can be changed inside of the scraping.py and the streamlit_interface.py file to accomodate for your needs. <br>

```py
# Start date should be set to your preferred start date
start_date=datetime(2023, 9, 19),
# Schedule interval can be set to you preferred interval
schedule_interval='0 20 * * *',
# Enable of Disable catchup
catchup=True
```

**There are 2 DAG's included in this pipeline:**
1) Scraper DAG:
This DAG will scrape the Immo website and create a dataframe. After this the dataframe will be automatically cleaned and uploaded to a S3 bucket to store for future reference. These files are saved with a unique (day) identifier (f.e. xgb_model_2023-09-10.joblib)

2) Streamlit Webserver Dag:
This DAG will launch a Streamlit webpage where you can fill out 12 different features of the property and get a prediction of the price (using the latest model in the S3 bucket).



## Installation :hammer_and_wrench:

Clone the github repository to your local machine:
```
git clone 
```

Navigate to the project directory and launch the docker file using the following command:

```
docker compose up --build
```

### :bucket: Connecting the S3 bucket to the app <br>

Experiencing Bucket issues?<br>
If you are using **minio or AWS**, please make sure you have a **minio server running** in the background (or AWS S3 set-up) and that you create 2 buckets ```"cleandatas3"``` and ```"xgbmodels3"``` inside either AWS or minio (depending on what service you use to host the S3 buckets). See how to set up the connection of the buckets below.
<br>
<br>
**Note:** if you plan on using an **AWS S3 bucket**, you will have to add a new connection (ip and port) + AWS login credentials to the airflow config. This can be done when the airflow webserver is running > admin > connections. There will be a minio connection in the list already, but this will not be available for you to access. It's important to set the connection between the S3 bucket and airflow to get this pipeline to work.

Once you have the docker container running and the airflow app is active you will need to make sure the S3 bucket connection is set up correctly. Once on the airflow homepage go to Admin > Connections

**You will see a list of connections**
![Alt text](<images/Screenshot from 2023-09-22 10-24-21.png>)

**Create a new connection and fill out the following fields**
![Alt text](<images/Screenshot from 2023-09-22 10-24-55.png>)

Take a note of the Connection Id and change this value inside of the **hook variable** in the **scraping.py** file

```py
hook = S3Hook('CONNECTION_ID_HERE')
```

You will see 2 DAGs on the Airflow > DAGs page. Activate the Scraper DAG and wait for the last task to be finished. Then Activate the StreamlitWebserver DAG if you want to launch the streamlit app.
![Alt text](<images/Screenshot from 2023-09-22 10-29-54.png>)


*Snippet of the streamlit webpage:*
This application will allow you to calculate the House / Appartment price based on 12 different features of the property (prediction based on the latest trained model)
![Alt text](<images/Screenshot from 2023-09-22 10-31-54.png>)
