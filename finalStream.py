import json
import requests
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Power BI Streaming URL
POWERBI_URL = "https://api.powerbi.com/beta/8695b83d-c692-47ef-ad7a-376cbce3664f/datasets/03a7eb58-886b-40c5-ae0f-b295685804d8/rows?experience=power-bi&key=M%2BRgTbvzyrbeO5DwBbUEqF%2B5sAJjwTD8eoSuBfPBenrMyiVea8QXYLMXMZwHrljpvlUV4lCjgf04EqfMlOI0nA%3D%3D    "

sc = SparkContext(appName="UberLiveStream")
ssc = StreamingContext(sc, 10)

# Folder to watch
input_dir = "file:///workspace/test"
lines = ssc.textFileStream(input_dir)

def send_to_powerbi(records):
    if not records:
        return
    try:
        payload = json.dumps(records)
        headers = {"Content-Type": "application/json"}
        resp = requests.post(POWERBI_URL, data=payload, headers=headers)
        print("Power BI Response:", resp.status_code)
    except Exception as e:
        print("Error sending:", e)

def process_rdd(rdd):
    rows = rdd.map(lambda line: line.split(",")) \
              .map(lambda arr: {
                    "CustomerID": arr[0],          
                    "PickupLocation": arr[1],       
                    "BookingValue": float(arr[2])   
              }).collect()

    if rows:
        print("Sending:", rows)
        send_to_powerbi(rows)

lines.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination()
