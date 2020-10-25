# Boston Crimes Map
Analyses data set https://www.kaggle.com/AnalyzeBoston/crimes-in-boston and write aggregations to parquet. 

## Usage
Build in ide and submit:
```
SPARK_DIR=${PATH_TO_SPARK} \
CRIME_CSV=${PATH_TO_CRIMES_CSV}
CODE_CSV=${PATH_TO_OFFENSE_CODES_CSV}
OUTPUT_PARQUET=${PATH_TO_PARQUET_OUTPUT_DIR}
make submit
```