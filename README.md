# dane_county_parcels_dbt
Compiling parcel data from Dane County in dbt

#What needs to be done?
- Initialize duckdb database
- Initialize dbt project
- Script python retrieval of parcel data
  - Figure out how we hold raw data in duckdb
- dbt model to pull parcels into single table
- Look at Jim's/Blake's clean-up steps, capture in dbt transform