# 🚖 Uber ETL Pipeline & Real-Time Streaming Dashboard

This project demonstrates a complete **Big Data ETL pipeline using PySpark** along with **real-time streaming integration** and a **Power BI Dashboard**.

---

## 📌 Project Overview

This project performs:

### **1️⃣ Data Integration from Multiple Sources**
- CSV (Uber India Trips)
- CSV with different format norms (Uber Europe Trips)
- JSON (Driver Dataset)
- TXT (City Master Data)
- Parquet (Vehicle Dataset)
- ⚡ Real-time Streaming CSV (Pickup Location, Booking Value, CustomerID)

### **2️⃣ PySpark ETL Pipeline**
- Extraction from multi-format data sources  
- Cleaning, transformation, type casting  
- Handling different date formats  
- Adding Region Labels (India / Europe)  
- Merging datasets  
- Generating final clean datasets:
  - `final_output/uber_final`
  - `final_output/india_final`
  - `final_output/europe_final`
  - `final_output/drivers_final`
  - `final_output/cities_final`
  - `final_output/vehicles_final`

### **3️⃣ Real-Time Streaming**
A PySpark Streaming job reads incoming `.csv` files from `/workspace/test` and pushes live data to a **Power BI Streaming Dataset** through a REST API.

✔ Live charts created in Power BI include:
- Real-Time Bar Chart (Pickup Location vs Booking Value)
- Gauge (Latest Ride Fare)
- Card (Latest Booking Value)
- Real-Time Booking Value by Customer

---

## 📊 Power BI Dashboard

A full interactive dashboard is included in the repo:
- **Uber_Dashboard.pbix**  
It contains:
- Booking trends  
- Revenue comparison  
- Cancellation analysis  
- Region-wise KPIs  
- Real-time live streaming visuals  

---

## 🛠️ How to Run This Project

### **1️.Clone the Repository**
bash
git clone https://github.com/ennayat-02/uber-etl-pipeline.git
cd uber-etl-pipeline


2. Run the PySpark ETL Pipeline

Inside Docker Spark terminal:

python etl.py

Outputs saved in:

/workspace/final_output/


3.2. Start Real-Time Streaming
python FileStreamExample.py

3. Push Streaming Data

Add files inside:
/workspace/test/

Example in bash: echo "C1004,Hyderabad,75.20" > /workspace/test/live1.csv
