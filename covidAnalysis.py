import findspark
findspark.init()

from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName("COVID-19 Data Analysis").getOrCreate()

if 'spark' in locals() and isinstance(spark,SparkSession):
    print("SparkSession created successfully.")
else:
    print("Failed to create SparkSession.")

# Read the COVID-19 data from the provided URL
vaccination_data = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/KpHDlIzdtR63BdTofl1mOg/owid-covid-latest.csv')    
vaccination_data.head()

print("Displaying the first 5 records of the vaccination data:")
columns_to_display = ['continent', 'total_cases', 'total_deaths', 'total_vaccinations', 'population']
# Show the first 5 records
print(vaccination_data[columns_to_display].head()) 

 # Convert to Spark DataFrame
spark_df = spark.createDataFrame(vaccination_data) 

print("Schema of the Spark DataFrame:")
spark_df.printSchema()
# Print the structure of the DataFrame (columns and types)

# List the names of the columns you want to display
columns_to_display = ['continent', 'total_cases', 'total_deaths', 'total_vaccinations', 'population']
# Display the first 5 records of the specified columns
spark_df.select(columns_to_display).show(5)


print("Displaying the 'continent' and 'total_cases' columns:")
# Show only the 'continent' and 'total_cases' columns
spark_df.select('continent', 'total_cases').show(5)  

print("Filtering records where 'total_cases' is greater than 1,000,000:")
 # Show records with more than 1 million total cases
spark_df.filter(spark_df['total_cases'] > 1000000).show(5) 

from pyspark.sql import functions as F

spark_df_with_percentage = spark_df.withColumn(
    'death_percentage', 
    (spark_df['total_deaths'] / spark_df['population']) * 100
)
spark_df_with_percentage = spark_df_with_percentage.withColumn(
    'death_percentage',
    F.concat(
        # Format to 2 decimal places
        F.format_number(spark_df_with_percentage['death_percentage'], 2), 
        # Append the percentage symbol 
        F.lit('%')  
    )
)
columns_to_display = ['total_deaths', 'population', 'death_percentage', 'continent', 'total_vaccinations', 'total_cases']
spark_df_with_percentage.select(columns_to_display).show(5)

print("Calculating the total deaths per continent:")
# Group by continent and sum total death rates
spark_df.groupby(['continent']).agg({"total_deaths": "SUM"}).show()

from pyspark.sql.functions import pandas_udf
# Define the return type of the UDF
@pandas_udf("int")  
# Function definition
def convert_total_deaths(s: pd.Series) -> pd.Series: 
    return s * 1  
# Here you can define any transformation you want
# Register the UDF with Spark
spark.udf.register("convert_total_deaths", convert_total_deaths)


# Create a new temporary SQL view of the DataFrame
spark_df.createTempView("data")

# Use UDF in a SQL query
print("Using UDF to convert total_deaths:")
spark.sql("SELECT continent, total_deaths, convert_total_deaths(total_deaths) as converted_total_deaths FROM data").show()

print("Displaying all records from the data table:")
# Show all records
spark.sql("SELECT * FROM data").show()

print("Displaying continent with total vaccinated more than 1 million:")
# SQL filtering
spark.sql("SELECT continent FROM data WHERE total_vaccinations > 1000000").show()