### Date created
2022-12-19


### Udacity course project: Data Engineering Capstone Project

 

### General Description
This capstone project contains a data pipeline implementation that transforms source data into the target data model for the US immigration data analysis.

The project uses AWS S3 data lake for storing low and high priority data and Spark cluster thar runs ETL pipeline logic transforming original data into analytics tables for the subsequent use by the analytics teams.

For this project, we use a star schema optimized for queries on immigration analysis. According to the selected schema we'll build one fact table which contains measures for the immigration data and several dimension tables which contain attributes describing our target entities.

For our fact table - **fact_i94_history** - we'll take i94 immigration data records and combine them with average temperatures in the city of entry for the month of the arrival event.

We will use two dimensions of aggregation for our fact data:

**dim_time** - timestamps of records in our fact_i94_history table broken down into specific units (day, week, month, year, etc.)

**dim_cities** - List of the U.S. cities with the most important statistical attributes on them (demography, number of airports, etc.)

Using this data model we'll be able to build some useful analytical queries:
- Finding relationships between the flow of immigrants and the demographic composition of the population
- Finding seasonal changes in immigrant flows depending on time of the year and average temperatures
etc.

Our pipeline will include the following steps:

1. **Data extraction from external sources**
    - Load all required datasets in various formats from external data sources
    - Narrow datasets and create schema for source tables
    - Save raw source data tables to the S3 data lake (in parquet format)

2. **Data cleaning, aligning and source quality checks**
    - Load source data tables from S3 data lake
    - Drop rows with missing values critical for the consistency
    - Further process source data (transform table shapes, data fields etc.)
    - Quality checks: eliminating duplicate rows and rows with incorrect combination of field values
    - Save preprocessed source tables to the S3 data lake (in parquet format)
    
3. **Forming the target data model and final quality checks**
    - Creating fact table (fact_i94_history) and dimension tables (dim_time and dim_cities) using prepared source tables
    - Save final fact and dimension tables to parquet files in the S3 data lake (in parquet format)


### Data sources

**Main dataset**: I94 Immigration Data from the US National Tourism and Trade. It contains statistcs on immigration to the USA (point of entry and detailed info on immigrants).

Source: https://travel.trade.gov/research/reports/i94/historical/2016.html

**Supplementary datasets**:

_U.S. City Demographic Data_: This data comes from OpenSoft and contains various population statistics on every US city.

Source: https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/

_Airport Code Table_: This dataset contains airport codes, geo references and corresponding cities.

Source: https://datahub.io/core/airport-codes#data

_World Temperature Data_: This dataset comes from Kaggle and contains various temperature statistics for a large number of world cities.

Source: https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data

**Additional tables**:
Following support tables were derived from source data dictionary for I94 Immigration Data (contained in file **I94_SAS_Labels_Descriptions.SAS**):

1. _countries.csv_: Dictionary for country from i94cit and i94res columns

2. _i94ports.csv_: Dictionary for link between **i94port** field and city of entrance.

3. _i94mode.csv_: Dctionary for text representation of border cross method in **i94mode** field 

4. _i94visa.csv_: Dictionary for text representation of visa type in **i94visa** field 

5. _us_states.csv_: Dictionary for full names of US States


### Credits

**This entire project is based on learning materials from Udacity:**
https://learn.udacity.com/
