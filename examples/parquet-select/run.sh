#!/bin/bash

export ENV_INPUT_PARQUET_FILENAME=./sample.d/input.parquet
export ENV_OUTPUT_PARQUET_FILENAME=./sample.d/output.parquet
export ENV_COLUMN_NAMES=timestamp,status

geninput(){
	echo generating input file...

	mkdir -p sample.d

	exec 11>&1
	exec 1>./sample.d/tmp.csv

	echo timestamp,severity,status,body
	echo 2025-09-29T01:00:43.012345Z,INFO,200,apt update done
	echo 2025-09-28T01:00:43.012345Z,WARN,500,apt update failure

	exec 1>&11
	exec 11>&-

	exec 11>&1
	exec 1>./sample.d/tmp.parquet.schema.txt

	echo '
	  message spark_schema {
		  REQUIRED BINARY timestamp(UTF8);
		  REQUIRED BINARY severity(UTF8);
		  REQUIRED BINARY status(UTF8);
		  REQUIRED BINARY body(UTF8);
	  }
	'

	exec 1>&11
	exec 11>&-

	which parquet-fromcsv | fgrep -q parquet-fromcsv || exec sh -c '
		echo parquet-fromcsv missing.
		echo you can install it using cargo install parquet --bin ...
		exit 1
	'

	parquet-fromcsv \
		--has-header \
		--schema ./sample.d/tmp.parquet.schema.txt \
		--input-file ./sample.d/tmp.csv \
		--output-file "${ENV_INPUT_PARQUET_FILENAME}"
}

test -f "${ENV_INPUT_PARQUET_FILENAME}" || geninput
./parquet-select

echo 'input parquet'
parquet-read --json "${ENV_INPUT_PARQUET_FILENAME}" |
	jq -c

echo
echo 'output parquet'
parquet-read --json "${ENV_OUTPUT_PARQUET_FILENAME}" |
	jq -c
