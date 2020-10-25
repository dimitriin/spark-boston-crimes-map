PROJECT_DIR ?= $(shell pwd)
SPARK_DIR ?= ${PROJECT_DIR}/.spark
CRIME_CSV ?= ${PROJECT_DIR}/.data/crime.csv
CODE_CSV ?= ${PROJECT_DIR}/.data/offense_codes.csv
OUTPUT_PARQUET ?= ${PROJECT_DIR}/.data/output

submit:
	${SPARK_DIR}/bin/spark-submit --master local[*] --class com.example.BostonCrimesMap \
	./target/scala-2.11/boston_crime_map-assembly-0.1.jar \
	${CRIME_CSV} \
	${CODE_CSV} \
	${OUTPUT_PARQUET}
.PHONY: submit