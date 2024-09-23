# COVID-19 Data Analysis with PySpark

## Project Overview
This project aims to perform data analysis on the latest COVID-19 dataset using PySpark and Pandas. The data includes COVID-19 statistics like total cases, total deaths, total vaccinations, and population data from various countries and continents. The analysis explores various features such as calculating death percentages, filtering data based on conditions, and leveraging SQL queries within Spark for efficient large-scale data processing.

The project demonstrates how to:
1. Initialize PySpark and create a SparkSession.
2. Read a CSV file using Pandas and convert it into a Spark DataFrame.
3. Perform filtering, column selection, and aggregation operations on the data.
4. Use Spark SQL for data querying.
5. Leverage user-defined functions (UDFs) in Spark.

### Repository Name
`COVID-19-Data-Analysis-PySpark`

## Project Steps

### 1. PySpark Initialization and Data Loading
- The project starts by initializing a SparkSession, which is essential for performing operations using Spark.
- The COVID-19 data is read from a CSV file hosted on a public URL using Pandas and then converted into a Spark DataFrame for further operations.

### 2. Initial Exploration
- Display the first 5 rows of the dataset and select important columns such as `continent`, `total_cases`, `total_deaths`, `total_vaccinations`, and `population` for further analysis.

### 3. Spark DataFrame Operations
- Display the schema of the Spark DataFrame.
- Perform a column selection on the DataFrame and display the selected columns.
- Filter the data to show only those records where `total_cases` exceeds 1,000,000.

### 4. Adding New Columns (Transformation)
- A new column `death_percentage` is created, calculating the percentage of deaths relative to the population.
- The death percentage is formatted to two decimal places and appended with a '%' symbol.

### 5. Aggregation and Grouping
- The project aggregates the total deaths by each continent, using Spark's `groupby` and aggregation functions.

### 6. Using User-Defined Functions (UDF)
- A Pandas UDF is defined and registered to Spark, which can be used within SQL queries.
- The UDF is then applied to convert and display the total deaths using a SQL query in Spark.

### 7. SQL Queries on Spark DataFrame
- Several SQL queries are executed to filter and display specific data, such as continents with more than 1 million vaccinations and converting total death numbers using the UDF.

## Key Libraries and Tools
- **PySpark**: A Python API for Apache Spark used for large-scale data processing and distributed computing.
- **Pandas**: A Python library used for data manipulation and analysis.
- **Spark SQL**: Spark's module for working with structured data using SQL queries.
- **User-Defined Functions (UDFs)**: Custom functions written in Python and used in Spark SQL queries.

## Dataset
The dataset used in this project is fetched from an online CSV source containing the latest COVID-19 statistics, including:
- Continent names
- Total cases, deaths, vaccinations
- Population data for different countries

### Example Data Operations:
1. **Filtering**: Records with `total_cases` greater than 1 million.
2. **Transformation**: Creating a new column for death percentage.
3. **Aggregation**: Summing up total deaths by continent.
4. **SQL Queries**: Running SQL queries using Spark SQL to analyze the data.

### Conclusion
This project highlights the use of PySpark for large-scale data analysis and provides an overview of performing essential operations on real-world COVID-19 data. The project is designed to be scalable and demonstrates how Spark handles large datasets efficiently, making it an ideal tool for big data analytics.
