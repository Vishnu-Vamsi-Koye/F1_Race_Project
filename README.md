# Project Overview
This project aimed at extracting, transforming, and storing data from Ergast websites using a combination of Python, PySpark, and SQL on Azure cloud resources. The key objective was to convert raw CSV files from diverse tables into various file formats, with a focus on JSON and Parquet. The process involved comprehensive transformations to enhance data accessibility and usability.
Tools and Technologies Used
# Languages:
•	Python

•	PySpark

•	SQL
# Platforms:
•	Azure Cloud Resources

•	Azure Data Lake

•	Azure Data Factory

•	Azure Key Vault

•	Azure Databricks

# Project Execution
Data Extraction and Conversion
1.	Data Extraction:
   Raw CSV files were sourced from Ergast websites, providing a foundational dataset for further processing.
  	
2.	File Format Conversion:
   The raw data underwent a transformation process, resulting in the conversion of data into JSON files for initial analysis.
Data Transformation and Storage

3.	Data Transformation:
   Comprehensive transformations were applied to the data to meet specific project requirements and enhance its analytical value.
4.	Storage in Parquet Files:
   Processed data was stored in Parquet files, ensuring efficient storage and retrieval.
5.	Azure Data Lake Integration:
   A designated transformation container in Azure Data Lake was utilized for storing the Parquet files, ensuring secure and scalable storage.
Enhancing Accessibility and SQL Integration
6.Restricting Access:
  Access to the data was restricted exclusively to downstream teams, facilitated through the use of Parquet files.
7.	Table Format Modification:
   Modifications were made to the transformations table to enable data retrieval in table format, enhancing accessibility for downstream teams using SQL queries.
# Folder Structure Overview
Setup: 

•	Notebooks for setting up environments and access necessary for project development.


Includes: 

•	Notebooks storing essential functions created during the project.

•	Configuration notebook defining project settings.

Utils:

•	Notebooks for creating databases for various layers (raw, transformation, presentation).

Demo:

•	Ten notebooks demonstrating SQL concepts, aggregations, and Delta Lake functionality.

Raw:

•	Contains the "circuits.csv" file for use in demonstration notebooks.

Ingestion of Data:

•	Four types of ingestion folders with notebooks covering initial data ingestion, basic transformations, data paths definition, incremental data load methods, and conversion to Delta Lake format.

Transformations:

•	Notebooks transforming data from the Bronze layer to the Gold layer for analysis and solution presentation.

Analysis:

•	Notebooks presenting solutions to analysis questions, including:

•	Dominant driver over a decade

•	Dominant teams over a decade

•	Race results over a specific time period

# Conclusion
This project successfully transformed raw data from Ergast websites into a structured and accessible format within the Azure environment. Leveraging Azure cloud resources, the project optimized data processing, storage, and accessibility. The folder structure provides a clear organization of notebooks, functions, and data for easy collaboration and maintenance. The enhanced data accessibility through SQL queries further ensures the usability of the transformed data for downstream teams. Overall, the project contributes to efficient data management and analysis capabilities within the Azure ecosystem.

