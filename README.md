# Data Engineering Capstone Project: ZILLOW ANALYTICS

## Project Summary
This project is to create a database that stores key information about the real estate market of 3 counties in the US.

It also provides a summary report about the number of transactions and total transaction value per month per county.

## Project Scope
- Extract raw data stored in **S3** (public access)
- Use **PySpark** (for fact) and **Pandas** (for dimension) to clean and format the data, store back to S3
- Copy the cleaned data to **Redshift**
- Create analytic table at aggregation level by fetching data from *Redshift* using *PySpark*, store back to *S3* and *Redshift*

## Data Sources
This project draws from 2 different data sources provided by [Kaggle and Zillow](https://www.kaggle.com/c/zillow-prize-1/)

* **Zillow data dimension directory** (1 file, 7 sheets, format .xlsx): dimension data for key attributes in the detail data set
* **Transaction data** (2 files, format json, total 167,888 records): info about time, transaction and log error between house estimated price and the actual sale price
* **Property details data** (2 files, format csv, total 5,970,434 records): details about all properties listed on Zillow in the 3 selected counties

## Data Cleaning

* **Dimension tables**: extract data from each excel sheet in S3 and import into predefined tables on Redshift. There are *not null* constraints to ensure there is no null ids
* **Fact tables**:
    * *Transactions*: convert transaction date into timestamp, rename and reorder columns
    * *Property details*: extract only necessary columns, convert columns to proper data types, drop any row with null id

## Data Model
### Conceptual Data Model
![Data Model](/zillow_data_model.png)

The purpose of the model is to look up property info when it's necessary and analyze transactions by connecting transaction data with property detail data.

We're using Relational database with design of Star Schema here since the dataset is fairly simple and already provided with mappings between relations. The schema consists of 3 fact tables referencing 7 dimension tables.

With this model, the users can:
* Look up attributes of the properties (what heating system used, what type of land, what type of construction...)
* Link a transaction with a property and all attributes related to it
* Calculate property's sold price based on log_error (transaction table) and tax value (property detail)
* Report on number of transactions and total per day/month/year, for any of the attributes showing up in the dimension tables

### Data Dictionary
There are 7 dimension tables and 3 fact tables:
* **Dimension tables** provides mapping for some critical attributes of properties
    * **heating_or_system_type**: id (int), heating_or_system (varchar)
    * **property_land_use_type**: id(int), property_land_use(varchar)
    * **story_type**: id(int), story (varchar)
    * **air_conditioning_type**: id(int), air_conditioning(varchar)
    * **architecture_style_type**: id(int), architectural_style(varchar)
    * **type_construction_type**: id(int), type_construction(varchar)
    * **building_class_type**: id(int), building_class(varchar)
    
* **Fact tables** provides information about property info, transactions and aggregated report of transaction per month
    * **property_transactions**: parcel_id(bigint), log_error(varchar), transaction_date(date)
    * **property_details**: parcel_id(bigint), air_conditioning_type_id (int), architectural_style_type_id (int), bathroom_count (int), bedroom_count (int), building_class_type_id (int), calculated_finished_square_feet (int), heating_or_system_type_id (int), property_land_use_type_id (int), region_id_city (int), region_id_county (int), region_id_zip (int), room_count (int), story_type_id (int), type_construction_type_id (int), year_built (int), tax_value_dollar_count (bigint)
    * **transactions_by_month**: region_id_county (int), year(int), month(int), transaction_count(bigint), transaction_value(float)

### Data Quality Checks

* Integrity constraints on the relational database (e.g., all tables on Redshift have data types, not null contraints). This check was performed in the **sql_queries.py** file
* Quality check functions: check for number of columns, type of columns and if there is any null value for id columns. This check was defined as functions and performed in the **process_transactions_table.py** and **process_details_table.py**

## Addressing Other Scenarios

### Current choice of tools and technologies
For smaller data sets (dimensions), pandas was used as it's faster to read and write data directly to Redshift.
For fact data, since there is a large volume of records to be processed, **Apache Spark** was used as it provides fast and in-memory data processing. It also provides the capabilities to run analyses and machine learning, which could be beneficial especially for this dataset (house price prediction). The data was then stored back to S3 and then batch-copied into Redshift for faster writing. 

Dimension data can be updated upon change. 
Fact data can be updated monthly. However it can also be updated more often depends on the need of the business. 

### Other scenarios
- **The data was increased by 100x** since Redshift can handle heavy reading and writing, one thing that i would change is to increase the number of nodes to cope up with the growing needs of data access
- **The data populates a dashboard that must be updated on a daily basis by 7am every day** the transactions script need to be adjust to append only yesterday's data. Then daily ETL job using Airflow or any other suitable tools can be set up to have that portion fetched into db. If the job failed, we would only miss data of that one day
- **The database needed to be accessed by 100+ people** increase the number of nodes to cope up with the growing needs of data access. Alse leverage AWS credentials to have the users properly set up to access Redshift

## Executing Project
The following steps can executed sequentially or separately depending on the current need. It also requires that the user has a Redshift cluster set up before running the script. You also need to change the path of Redshift JDBC driver

* Step 1: add aws credentials, endpoint and role arn into config file (dwh.cfg)
* Step 2: create and process dimension tables by running this line
```bash
python process_dimension_tables.py
```
* Step 3: create fact tables
```bash
python create_fact_tables.py
```
* Step 4: process transaction data
```bash
python process_transactions_table.py
```
* Step 5: process property detail data
```bash
python process_details_table.py
```
* Step 6: create analytics report
```bash
python create_analytics_table.py
```
