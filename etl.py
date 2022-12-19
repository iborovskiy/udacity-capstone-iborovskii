import pandas as pd
from pyspark.sql import SparkSession

def load_source_data(spark, input_data, output_data):
    """
    - Load all required datasets in various formats from external data sources
    - Narrow datasets and create schema for source tables
    - Save raw source data tables to the S3 data lake (in parquet format)
    """
    print('Step 1 - Data extraction from external sources - started.')
    print()
    print('Reading dictionary tables...')
    # Read in country dictionary from 'countries.csv'
    country_df = spark.read.option("header", True).option("delimiter", ";").csv(input_data['dict_tables'] + 'countries.csv')
    print(f'{country_df.toPandas().shape[0]:,d} records were successfully loaded from countries.csv')
    # Read in port dictionary from 'i94ports.csv'
    port_df = spark.read.option("header", True).option("delimiter", ";").csv(input_data['dict_tables'] + 'i94ports.csv')
    print(f'{port_df.toPandas().shape[0]:,d} records were successfully loaded from i94ports.csv')
    # Read in port dictionary from 'i94mode.csv'
    mode_df = spark.read.option("header", True).option("delimiter", ";").csv(input_data['dict_tables'] + 'i94mode.csv')
    print(f'{mode_df.toPandas().shape[0]:,d} records were successfully loaded from i94mode.csv')
    # Read in port dictionary from 'i94visa.csv'
    visa_df = spark.read.option("header", True).option("delimiter", ";").csv(input_data['dict_tables'] + 'i94visa.csv')
    print(f'{visa_df.toPandas().shape[0]:,d} records were successfully loaded from i94visa.csv')
    # Read in port dictionary from 'us_states.csv'
    states_df = spark.read.option("header", True).option("delimiter", ";").csv(input_data['dict_tables'] + 'us_states.csv')
    print(f'{states_df.toPandas().shape[0]:,d} records were successfully loaded from us_states.csv')
    
    print('Reading i94 immigration dataset...')
    # Read in i94 immigration SAS data
    i94_full_df = spark.read.format('com.github.saurfang.sas.spark').load(input_data['i_94_immig'])
    print(f'{i94_full_df.count():,d} records were successfully loaded from i94 immigration dataset.')

    print('Reading U.S. cities demographics dataset...')
    cities_pop_df = spark.read.option("header", True).option("delimiter", ";").csv(input_data['demographic'] + 'us-cities-demographics.csv')
    cities_pop_df = cities_pop_df.toDF('city', 'state', 'median_age', 'male_population', 'female_population', 'total_population', 'number_of_veterans',
                                      'foreign_born', 'avg_household_size', 'state_code', 'race', 'count')
    print(f'{cities_pop_df.count():,d} records were successfully loaded from U.S. cities demographics dataset.')
    
    print('Reading airport codes dataset...')
    airports_df = spark.read.option("header", True).csv(input_data['airports'] + 'airport-codes_csv.csv')
    print(f'{airports_df.count():,d} records were successfully loaded from airport codes dataset.')
    
    print('Reading global temperatures by cities dataset...')
    weather_df = spark.read.option("header", True).csv(input_data['temperature'])
    print(f'{weather_df.count():,d} records were successfully loaded from global temperatures by cities dataset.')

    # create a temporary views against which you can run SQL queries
    i94_full_df.createOrReplaceTempView("i94_table")
    country_df.createOrReplaceTempView("country_table")
    port_df.createOrReplaceTempView("port_table")
    mode_df.createOrReplaceTempView("mode_table")
    visa_df.createOrReplaceTempView("visa_table")
    states_df.createOrReplaceTempView("states_df")
    cities_pop_df.createOrReplaceTempView("cities_pop_table")
    airports_df.createOrReplaceTempView("airports_table")
    weather_df.createOrReplaceTempView("weather_table")

    # narrow datasets and create schema for source tables
    print()
    print('Narrowing datasets and creating correct schema for each source table...')
    # For i94_source_table we perform following tasks:
    # 1. Set correct data types for the fields
    # 2. Translate i94res and i94cit codes into country names
    # 3. Translate i94port code into city name, state names and state abbreviation name
    # 4. Convert country_of_residence, country_of_citizenship, city_of_entry, state_of_entry_code
    #    and state_of_entry_full into capital letters
    # 5. Translate i94mode code into border cross method text name
    # 6. Translate i94visa code into type of visa text name
    # 7. Convert dates from SAS format (with base - 1960-01-01)
    # 8. Add year, month and day fields for parquet partitions
    i94_full_source_table = spark.sql('''
        SELECT CAST(cicid AS LONG) id, UPPER(c1.country_name) country_of_residence, UPPER(c2.country_name) country_of_citizenship,
                UPPER(p.port_location) city_of_entry, UPPER(p.state) state_of_entry_code, UPPER(s.state) state_of_entry_full, 
                m.border_cross_method border_cross_method, v.visa_type type_of_visa,
                i.visatype class_of_admission, i.gender gender, CAST(i.i94bir AS LONG) age,
                date_add(to_date('1960-01-01'), CAST(arrdate AS LONG)) arr_date, date_add(to_date('1960-01-01'), CAST(depdate AS LONG)) dep_date,
                YEAR(CAST(date_add(to_date('1960-01-01'), CAST(arrdate AS LONG)) AS DATE)) year,
                MONTH(CAST(date_add(to_date('1960-01-01'), CAST(arrdate AS LONG)) AS DATE)) month,
                DAY(CAST(date_add(to_date('1960-01-01'), CAST(arrdate AS LONG)) AS DATE)) day
        FROM i94_table i
        JOIN country_table c1
        ON CAST(i.i94res AS LONG) = c1.code
        JOIN country_table c2
        ON CAST(i.i94cit AS LONG) = c2.code
        JOIN port_table p
        ON i.i94port = p.code
        JOIN mode_table m
        ON CAST(i94mode AS LONG) = m.code
        JOIN visa_table v
        ON CAST(i94visa AS LONG) = v.code
        JOIN states_df s
        ON p.state = s.code
    ''')
    i94_full_source_table.createOrReplaceTempView("i94_source_table")
    print(f'{i94_full_source_table.count():,d} records were successfully processed into i94_source_table table.')
    
    # For cities_pop_source_table we perform following tasks:
    # 1. Set correct data types for the fields 
    # 2. Convert city, state and state_code into capital letters
    cities_pop_source_table = spark.sql('''
        SELECT UPPER(city) city, UPPER(state) state, CAST(median_age AS DOUBLE) median_age,
                CAST(male_population AS LONG) male_population, CAST(female_population AS LONG) female_population,
                CAST(total_population AS LONG) total_population, CAST(number_of_veterans AS LONG) number_of_veterans,
                CAST(foreign_born AS LONG) foreign_born, CAST(avg_household_size AS DOUBLE) avg_household_size,
                UPPER(state_code) state_code, race, CAST(count AS LONG) count
        FROM cities_pop_table c
    ''')
    cities_pop_source_table.createOrReplaceTempView("cities_pop_source_table")
    print(f'{cities_pop_source_table.count():,d} records were successfully processed into cities_pop_source_table table.')


    # For airports_source_table we perform following tasks:
    # 1. Set correct data types for the fields
    # 2. Extract state abbreviation from iso_region field
    # 3. Split coordinates field into lat and lon fields
    # 4. Convert municipality and iso_region into capital letters
    # 5. Drop closed airports and those that are not in the US
    airports_source_table = spark.sql('''
        SELECT type, name, CAST(elevation_ft AS LONG) elevation_ft, UPPER(SPLIT(iso_region, '-')[1]) iso_region,
                UPPER(municipality) municipality, gps_code, CAST(SPLIT(coordinates,',')[0] AS DOUBLE) lon, CAST(SPLIT(coordinates,',')[1] AS DOUBLE) lat
        FROM airports_table a
        WHERE iso_country = 'US' AND type != 'closed'
    ''')
    airports_source_table.createOrReplaceTempView("airports_source_table")
    print(f'{airports_source_table.count():,d} records were successfully processed into airports_source_table table.')

    # For weather_source_table we perform following tasks:
    # 1. Set correct data types for the fields
    # 2. Convert country and city into capital letters 
    # 3. Drop cities that are not in the US
    # 4. Drop records before 1960
    # 5. Convert longitude and latitude from string representation (like 39.38N - 89.48W) into real numbers.
    #    For simplicity of transformation we assume that all cities in dataset will have north latitude (> 0) 
    #    and west longitude (< 0) since we've dropped all cities from outside of the U.S.
    # 6. Add year, month and day fields for parquet partitions
    weather_source_table = spark.sql('''
        SELECT CAST(dt AS DATE) dt, YEAR(CAST(dt AS DATE)) year, MONTH(CAST(dt AS DATE)) month, DAY(CAST(dt AS DATE)) day, CAST(AverageTemperature AS DOUBLE) avg_temperature,
        UPPER(city) city, UPPER(country) country, CAST(SUBSTRING(longitude,1,LENGTH(longitude)-1) AS DOUBLE)*-1 lon, CAST(SUBSTRING(latitude,1,LENGTH(latitude)-1) AS DOUBLE) lat
        FROM weather_table w
        WHERE country = 'United States' AND YEAR(CAST(dt AS DATE)) >= 1960
    ''')
    weather_source_table.createOrReplaceTempView("weather_source_table")
    print(f'{weather_source_table.count():,d} records were successfully processed into weather_source_table table.')

    #write source tables into the data lake using parquet format
    print()
    print('Writing raw source tables into the data lake...')
    i94_full_source_table.write.partitionBy('year', 'month').mode('overwrite').\
                    parquet(output_data + 'source/i94/i94_records.parquet')
    print('i94_records.parquet was successfully saved in the data lake.')

    cities_pop_source_table.write.partitionBy('state_code').mode('overwrite').\
                    parquet(output_data + 'source/demographic/demographic.parquet')
    print('demographic.parquet was successfully saved in the data lake.')

    weather_source_table.write.partitionBy('year', 'month').mode('overwrite').\
                    parquet(output_data + 'source/weather/weather.parquet')
    print('weather.parquet was successfully saved in the data lake.')

    airports_source_table.write.partitionBy('iso_region').mode('overwrite').\
                    parquet(output_data + 'source/airports/airports.parquet')
    print('airports.parquet was successfully saved in the data lake.')

    print('Step 1 - Data extraction from external sources - successfully completed.')
    print('')

    
def process_source_data(spark, input_data, output_data):
    """
    - Load source data tables from S3 data lake
    - Drop rows with missing values critical for the consistency
    - Further process source data (transform table shapes, data fields etc.)
    - Quality checks: eliminating duplicate rows and rows with incorrect combination of field values
    - Save preprocessed source tables to the S3 data lake (in parquet format)
    """
    print('Step 2 - Data cleaning, aligning and source quality checks - started.')
    print()
    
    # Read source tables from the data lake
    print('Loading source tables from the data lake...')
    i94_full_source_table = spark.read.parquet(output_data + 'source/i94/i94_records.parquet')
    i94_full_source_table.createOrReplaceTempView("i94_source_table")
    i94_full_source_count = i94_full_source_table.count()
    print(f'{i94_full_source_count:,d} records were successfully loaded from the data lake into i94_source_table table.')
    cities_pop_source_table = spark.read.parquet(output_data + 'source/demographic/demographic.parquet')
    cities_pop_source_table.createOrReplaceTempView("cities_pop_source_table")
    cities_pop_source_count = cities_pop_source_table.count()
    print(f'{cities_pop_source_count:,d} records were successfully loaded from the data lake into cities_pop_source_table table.')
    weather_source_table = spark.read.parquet(output_data + 'source/weather/weather.parquet')
    weather_source_table.createOrReplaceTempView("weather_source_table")
    weather_source_count = weather_source_table.count()
    print(f'{weather_source_count:,d} records were successfully loaded from the data lake into weather_source_table table.')
    airports_source_table = spark.read.parquet(output_data + 'source/airports/airports.parquet')
    airports_source_table.createOrReplaceTempView("airports_source_table")
    airports_source_count = airports_source_table.count()
    print(f'{airports_source_count:,d} records were successfully loaded from the data lake into airports_source_table table.')

    # Drop rows with missing values critical for the consistency
    # and further process source data (transform table shapes, data fields etc.).
    print()
    print('Cleaning and transforming source tables...')
    
    # For i94_source_table we perform following tasks:
    # 1. drop rows with duplicate id field
    # 2. drop rows with missing values in id, state_of_entry_code, city_of_entry, arr_date
    # 3. drop rows with dep_date < arr_date
    i94_full_source_table = spark.sql('''
        SELECT DISTINCT *
        FROM i94_source_table i
        WHERE (id IS NOT NULL) AND (state_of_entry_code IS NOT NULL) AND
                (city_of_entry IS NOT NULL) AND (arr_date IS NOT NULL) AND
                (dep_date IS NULL OR dep_date >= arr_date)
    ''')
    i94_full_source_table.createOrReplaceTempView("i94_source_table")
    i94_full_processed_count = i94_full_source_table.count()
    print(f'i94_source_table was successfully preprocessed.')
    print(f'{(i94_full_source_count - i94_full_processed_count):,d} records were dropped from the table.')
    print(f'Total number of rows in preprocessed i94_source_table - {i94_full_processed_count:,d}.')
    
    # For cities_pop_source_table we perform following tasks:
    # 1. drop rows with missing values in city and state_code
    # 2. build pivot columns for different races in given city - state
    # 3. drop rows with duplicate combination of city and state_code fields
    # Step 1
    cities_pop_source_table = spark.sql('''
        SELECT *
        FROM cities_pop_source_table c
        WHERE (city IS NOT NULL) AND (state_code IS NOT NULL)
    ''')
    cities_pop_source_table.createOrReplaceTempView("cities_pop_source_table")
    # Steps 2 and 3
    cities_pop_source_table = spark.sql('''
        SELECT DISTINCT c.city city, c.state state, c.state_code state_code, c.median_age median_age, 
                        c.male_population male_population, c.female_population female_population,
                        c.total_population total_population, c.number_of_veterans number_of_veterans,
                        c.foreign_born foreign_born, c.avg_household_size avg_household_size,
                        c1.american_indian_and_alaska_native american_indian_and_alaska_native,
                        c2.asian asian, c3.white white, c4.hispanic_or_latino hispanic_or_latino,
                        c5.black_or_african_american black_or_african_american
        FROM cities_pop_source_table c
        LEFT JOIN (
            SELECT city, state, count as american_indian_and_alaska_native
            FROM cities_pop_source_table
            WHERE race = "American Indian and Alaska Native"
        ) c1
        ON c.city = c1.city AND c.state = c1.state
        LEFT JOIN (
            SELECT city, state, count as asian
            FROM cities_pop_source_table
            WHERE race = "Asian"
        ) c2
        ON c.city = c2.city AND c.state = c2.state
        LEFT JOIN (
            SELECT city, state, count as white
            FROM cities_pop_source_table
            WHERE race = "White"
        ) c3
        ON c.city = c3.city AND c.state = c3.state
        LEFT JOIN (
            SELECT city, state, count as hispanic_or_latino
            FROM cities_pop_source_table
            WHERE race = "Hispanic or Latino"
        ) c4
        ON c.city = c4.city AND c.state = c4.state
        LEFT JOIN (
            SELECT city, state, count as black_or_african_american
            FROM cities_pop_source_table
            WHERE race = "Black or African-American"
        ) c5
        ON c.city = c5.city AND c.state = c5.state
    ''')
    cities_pop_source_table.createOrReplaceTempView("cities_pop_source_table")
    cities_pop_processed_count = cities_pop_source_table.count()
    print(f'cities_pop_source_table was successfully preprocessed.')
    print(f'{(cities_pop_source_count - cities_pop_processed_count):,d} records were dropped from the table.')
    print(f'Total number of rows in preprocessed cities_pop_source_table - {cities_pop_processed_count:,d}.')

    # For airports_source_table we perform following tasks:
    # 1. drop rows with missing values in iso_region, municipality, lat and lon
    # 2. drop rows with duplicate rows
    # We don't need to check for duplicate combination of iso_region, municipality and name fields
    # as we'll build pivot table for quantity of the airports for given city-state combination
    airports_source_table = spark.sql('''
        SELECT *
        FROM airports_source_table a
        WHERE (municipality IS NOT NULL) AND (iso_region IS NOT NULL) AND 
        (lat is NOT NULL) AND (lon is NOT NULL)
    ''')
    airports_source_table.createOrReplaceTempView("airports_source_table")
    airports_processed_count = airports_source_table.count()
    print(f'airports_source_table was successfully preprocessed.')
    print(f'{(airports_source_count - airports_processed_count):,d} records were dropped from the table.')
    print(f'Total number of rows in preprocessed airports_source_table - {airports_processed_count:,d}.')

    # For weather_source_table we perform following tasks:
    # 1. drop rows with missing values in dt, avg_temperature, city, lat and lon
    # 2. drop duplicate rows
    # 3. join this table with airports_source_table using distance estimate 
    #    from city coordinates (lat-lon) to airport coordinates (lat-lon) 
    #    and evaluate state code for every city
    # We drop from the resulting table all cities without state code
    # as we cannot use them unambiguously in the future analysis
    # Steps 1 and 2
    weather_source_table = spark.sql('''
        SELECT DISTINCT *
        FROM weather_source_table w
        WHERE (avg_temperature IS NOT NULL) AND (dt IS NOT NULL) AND
                (city IS NOT NULL) AND (lat IS NOT NULL) AND (lon IS NOT NULL)
    ''')
    weather_source_table.createOrReplaceTempView("weather_source_table")    
    # Step 3
    tmp_loc_table = spark.sql('''
        SELECT DISTINCT w.city city, w.lat lat, w.lon lon
        FROM weather_source_table w
    ''')
    tmp_loc_table.createOrReplaceTempView("tmp_loc_table")
    DISTANCE_THRESHOLD = 1 # Distance threshold constant. Might be adjusted based on the number of dropped records in final table
    tmp_loc_table = spark.sql(f'''
        SELECT DISTINCT w1.city city, a.iso_region state, w1.lat lat, w1.lon lon
        FROM tmp_loc_table w1
        JOIN airports_source_table a
        ON SQRT((w1.lat-a.lat)*(w1.lat-a.lat)+(w1.lon-a.lon)*(w1.lon-a.lon)) < {DISTANCE_THRESHOLD} AND w1.city = a.municipality
    ''')
    tmp_loc_table.createOrReplaceTempView("tmp_loc_table")
    weather_source_table = spark.sql('''
        SELECT w.dt dt, w.year year, w.month month, w.day day, w.city, w1.state,
        w.lat, w.lon, AVG(w.avg_temperature) avg_temperature
        FROM weather_source_table w
        JOIN tmp_loc_table w1
        ON w.city = w1.city AND w.lat = w1.lat AND w.lon = w1.lon
        GROUP BY w.dt, w.year, w.month, w.day, w.city, w1.state, w.lat, w.lon    
    ''')
    weather_source_table.createOrReplaceTempView("weather_source_table")
    weather_processed_count = weather_source_table.count()
    print(f'weather_source_table was successfully preprocessed.')
    print(f'{(weather_source_count - weather_processed_count):,d} records were dropped from the table.')
    print(f'Total number of rows in preprocessed weather_source_table - {weather_processed_count:,d}.')

    #write prepared source tables into the data lake using parquet format
    print()
    print('Writing preprocessed source tables into the data lake...')

    i94_full_source_table.write.partitionBy('year', 'month').mode('overwrite').\
                    parquet(output_data + 'preprocessed/i94/i94_records.parquet')
    print('i94_records.parquet was successfully saved in the data lake.')

    cities_pop_source_table.write.partitionBy('state_code').mode('overwrite').\
                    parquet(output_data + 'preprocessed/demographic/demographic.parquet')
    print('demographic.parquet was successfully saved in the data lake.')

    weather_source_table.write.partitionBy('year', 'month').mode('overwrite').\
                    parquet(output_data + 'preprocessed/weather/weather.parquet')
    print('weather.parquet was successfully saved in the data lake.')

    airports_source_table.write.partitionBy('iso_region').mode('overwrite').\
                    parquet(output_data + 'preprocessed/airports/airports.parquet')
    print('airports.parquet was successfully saved in the data lake.')

    print('Step 2 - Data cleaning, aligning and source quality checks - successfully completed.')
    print('')


def check_model_quality(spark, model_data):
    """
    - Check if fact and dimension tables contain greater than zero records
    - Check if fact table contains dates missing in dim_time table
    Integrity constraints on the relational database (e.g., unique key, data type, etc.) were implemented in the previous steps of pipeline 
    
    Return:
    - True, if check have passed
    - False, otherwise
    """
    print()
    print('Final quality checks for the data model - started.')
    print()
    
    fact_i94_history_table = model_data[0]
    dim_cities_table = model_data[1]
    dim_time_table = model_data[2]
    
    if (fact_i94_history_table.count() == 0) or (dim_cities_table.count() == 0) or \
        (dim_time_table.count() == 0):
        return False
    
    fact_i94_history_table.createOrReplaceTempView("fact_i94_history")
    dim_cities_table.createOrReplaceTempView("dim_cities")
    dim_time_table.createOrReplaceTempView("dim_time")

    check_query = spark.sql('''
        SELECT f.arr_date, t.dt
        FROM fact_i94_history f
        LEFT JOIN dim_time t
        ON (YEAR(f.arr_date) = YEAR(t.dt)) AND (MONTH(f.arr_date) = MONTH(t.dt))
        WHERE t.dt IS NULL
    ''')
    if check_query.count() > 0:
        return False
    
    print('Final quality checks for the data model - successfully completed.')
    print('')
    return True


def build_data_model(spark, input_data, output_data):
    """
    - Load preprocessed source data tables from S3 data lake
    - Create fact table (fact_i94_history) and dimension tables (dim_time and dim_cities)
    - Perform quality checks on the data model
    - Save final fact and dimension tables to parquet files in the S3 data lake
    """
    print('Step 3 - Forming the target data model and final quality checks - started.')
    print()
    # Read source tables from the data lake
    print('Loading preprocessed source tables from the data lake...')
    i94_full_source_table = spark.read.parquet(output_data + 'preprocessed/i94/i94_records.parquet')
    i94_full_source_table.createOrReplaceTempView("i94_clean_source_table")
    print(f'{i94_full_source_table.count():,d} records were successfully loaded from the data lake into i94_clean_source_table table.')
    cities_pop_source_table = spark.read.parquet(output_data + 'preprocessed/demographic/demographic.parquet')
    cities_pop_source_table.createOrReplaceTempView("cities_pop_clean_source_table")
    print(f'{cities_pop_source_table.count():,d} records were successfully loaded from the data lake into cities_pop_clean_source_table table.')
    weather_source_table = spark.read.parquet(output_data + 'preprocessed/weather/weather.parquet')
    weather_source_table.createOrReplaceTempView("weather_clean_source_table")
    print(f'{weather_source_table.count():,d} records were successfully loaded from the data lake into weather_clean_source_table table.')
    airports_source_table = spark.read.parquet(output_data + 'preprocessed/airports/airports.parquet')
    airports_source_table.createOrReplaceTempView("airports_clean_source_table")
    print(f'{airports_source_table.count():,d} records were successfully loaded from the data lake into airports_clean_source_table table.')

    # Build dimension tables
    print()
    print('Building dimension tables...')
    
    # For dim_time table we perform the following tasks:
    # 1. extract unique date values from arr_date and dep_date fields of i94_clean_source_table and 
    #    from dt field of weather_clean_source_table and make a union of them
    # 2. add separate fields for year, month, day, weekday and week parts of date
    dim_time_table = spark.sql('''
        SELECT DISTINCT i1.arr_date dt, YEAR(i1.arr_date) year, MONTH(i1.arr_date) month, DAY(i1.arr_date) day,
                        DAYOFWEEK(i1.arr_date) dow, WEEKOFYEAR(i1.arr_date) week
        FROM i94_clean_source_table i1
        UNION
        SELECT DISTINCT i2.dep_date dt, YEAR(i2.dep_date) year, MONTH(i2.dep_date) month, DAY(i2.dep_date) day,
                        DAYOFWEEK(i2.dep_date) dow, WEEKOFYEAR(i2.dep_date) week
        FROM i94_clean_source_table i2
        WHERE i2.dep_date IS NOT NULL
        UNION
        SELECT DISTINCT w.dt dt, YEAR(w.dt) year, MONTH(w.dt) month, DAY(w.dt) day,
                        DAYOFWEEK(w.dt) dow, WEEKOFYEAR(w.dt) week
        FROM weather_clean_source_table w
    ''')
    dim_time_table.createOrReplaceTempView("dim_time")
    print(f'Dimension table dim_time with {dim_time_table.count():,d} records was successfully created .')

    # For dim_cities table we perform following tasks:
    # 1. extract all attributes from cities_pop_clean_source_table
    # 2. transform airports_clean_source_table into pivot table with number of airports per city - state combination
    # 3. add number_of_airports field in resulting dim_cities table 
    #    by joining municipality & iso_region composite key with city & state_code omposite key
    # Step 1
    dim_cities_table = spark.sql('''
        SELECT *
        FROM cities_pop_clean_source_table c
    ''')
    dim_cities_table.createOrReplaceTempView("dim_cities")
    # Step 2
    airports_pivot_table = spark.sql('''
        SELECT iso_region, municipality, COUNT(*) number_of_airports
        FROM airports_clean_source_table a
        GROUP BY iso_region, municipality
    ''')
    airports_pivot_table.createOrReplaceTempView("airports_pivot_table")
    # Step 3
    dim_cities_table = spark.sql('''
        SELECT dc.*, a.number_of_airports number_of_airports
        FROM dim_cities dc
        LEFT JOIN airports_pivot_table a
        ON (dc.city = a.municipality) AND (dc.state_code = a.iso_region)
    ''')
    dim_cities_table.createOrReplaceTempView("dim_cities")
    print(f'Dimension table dim_cities with {dim_cities_table.count():,d} records was successfully created .')

    # Build fact table
    print()
    print('Building fact table...')
    # For fact_i94_history table we perform following tasks:
    # 1. extract following attributes from i94_clean_source_table:
    #    id, country_of_residence, country_of_citizenship, city_of_entry, state_of_entry_code, border_cross_method,
    #    type_of_visa, class_of_admission, gender, age, arr_date, dep_date
    # 2. add fields for year, month and day parts of the arrival_date
    # 3. add avg_temperature field in resulting fact_i94_history table 
    #    by joining city_of_entry & state_of_entry_code & YEAR(arr_date) & MONTH(arr_date) composite key 
    #    with city & state & YEAR(dt) & MONTH(dt) omposite key
    # Steps 1 and 2
    fact_i94_history_table = spark.sql('''
        SELECT i.id id, i.country_of_residence country_of_residence, i.country_of_citizenship country_of_citizenship,
                i.city_of_entry city_of_entry, i.state_of_entry_code state_of_entry_code, 
                i.border_cross_method border_cross_method, i.type_of_visa type_of_visa,
                i.class_of_admission class_of_admission, i.gender gender, i.age age,
                i.arr_date arr_date, i.dep_date dep_date, YEAR(i.arr_date) year, MONTH(i.arr_date) month, DAY(i.arr_date) day
        FROM i94_clean_source_table i
    ''')
    fact_i94_history_table.createOrReplaceTempView("fact_i94_history")
    # Step 3
    fact_i94_history_table = spark.sql('''
        SELECT f.*, w.avg_temperature avg_temperature
        FROM fact_i94_history f
        LEFT JOIN weather_clean_source_table w
        ON (YEAR(f.arr_date) = YEAR(w.dt)) AND (MONTH(f.arr_date) = MONTH(w.dt)) AND
            (f.city_of_entry = w.city) AND (f.state_of_entry_code = w.state)
    ''')    
    fact_i94_history_table.createOrReplaceTempView("fact_i94_history")
    print(f'Fact table fact_i94_history with {fact_i94_history_table.count():,d} records was successfully created .')
    print()

    model_data = [fact_i94_history_table, dim_cities_table, dim_time_table]
    check_res = check_model_quality(spark, model_data)
    if check_res:
        print('Quality checks on the data model have successfully passed.')
    else:
        print('ERROR! Quality checks on the data model have failed!!!')
        return

    #write prepared fact and dimension tables into the data lake using parquet format
    print()
    print('Writing preprocessed fact and dimension tables into the data lake...')

    fact_i94_history_table.write.partitionBy('year', 'month').mode('overwrite').\
                    parquet(output_data + 'analytics/fact_i94_history/fact_i94_history.parquet')
    print('fact_i94_history.parquet was successfully saved in the data lake.')
    dim_cities_table.write.partitionBy('state_code').mode('overwrite').\
                    parquet(output_data + 'analytics/dim_cities/dim_cities.parquet')
    print('dim_cities.parquet was successfully saved in the data lake.')
    dim_time_table.write.partitionBy('year', 'month').mode('overwrite').\
                    parquet(output_data + 'analytics/dim_time/dim_time.parquet')
    print('dim_time.parquet was successfully saved in the data lake.')
    print('')

    print('Step 3 - Forming the target data model and final quality checks - successfully completed.')
    print('')


def main():
    """
    - Creates Spark sessions
    - Executes all steps of the ETL pipelie
    """
    # State your input paths for source datasets
    input_data = {
        "i_94_immig": "../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat",
        "demographic": "",
        "airports": "",
        "temperature": "../../data2/GlobalLandTemperaturesByCity.csv",
        "dict_tables": ""
    }
    # State your output s3 bucket for source data and processed fact and dimension tables  
    output_data = ""
    
    print('Starting or connecting to Spark...')
    spark = SparkSession.builder.\
                config("spark.jars.repositories", "https://repos.spark-packages.org/").\
                config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
                enableHiveSupport().getOrCreate()
    print('Spark session established successfully.')
    
    # Execute all stages of the ETL
    # Step 1. Data extraction from external sources
    load_source_data(spark, input_data, output_data)
    
    #Step 2. Data cleaning, aligning and source quality checks
    process_source_data(spark, input_data, output_data)

    #Step 3. Forming the target data model and final quality checks
    build_data_model(spark, input_data, output_data)   
   
    
if __name__ == '__main__':
    main()

#weather_source_table.write.parquet("sas_data")
#df_spark=spark.read.parquet("sas_data")