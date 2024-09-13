
with coalesce_fields as (
    --coalesce different field names over the years, remove extra "", trim extra space
    select parcelno,
        year_number as assessment_year,
        coalesce(attributes."Sum_Improv",attributes."SUM_IMP")::DOUBLE as improvement_value,
        coalesce(attributes."Sum_LandVa",attributes."SUM_LAND")::DOUBLE as land_value,
        coalesce(attributes."Assessed_A",attributes."SUM_ACRES")::DOUBLE as assessed_acres,
        trim(replace(coalesce(attributes."PropertySt",ltrim(attributes."STREET_NO")),'"',''))::VARCHAR(8) as street_number,
        trim(replace(coalesce(attributes."PropertyPr",attributes."STREET_DIR"),'"',''))::VARCHAR(8) as street_direction,
        trim(replace(coalesce(attributes."Property_1",attributes."STREET_NAM"),'"',''))::VARCHAR(32) as street_name, --Sometimes inclues street_type
        trim(replace(coalesce(attributes."Property_2",attributes."STREET_TYP"),'"',''))::VARCHAR(8) as street_type,
        trim(replace(coalesce(attributes."Property_3",attributes."UNIT_ID"),'"',''))::VARCHAR(8) as unit_number,
        trim(replace(coalesce(attributes."Property_5",attributes."CITY"),'"',''))::VARCHAR(32) as city,
        trim(replace(coalesce(attributes."PropertyZi",attributes."ZIP_CODE"),'"',''))::VARCHAR(8) as zip_code,
    from {{ source('raw','parcels_raw') }}
    where parcelno = '070929101015'
)

select parcelno,
    assessment_year,
    improvement_value,
    land_value,
    --really need to double check this window function
    ifnull(nullif(assessed_acres,0),max(assessed_acres) over (partition by parcelno order by assessment_year desc rows between unbounded preceding and current row)) as assessed_acres,
    street_number,
    street_direction,
    --make sure street_name doesn't end in street_type, assume street type is preceded by a space and ends the street name
    regexp_replace(street_name, concat(' ',street_type,'$'), '') as street_name,
    street_type,
    unit_number,
    --lazy window functions, assume city and zip are stable
    ifnull(upper(city),max(upper(city)) over (partition by parcelno)) as city,
    ifnull(zip_code,max(upper(zip_code)) over (partition by parcelno)) as zip_code,
from coalesce_fields
order by assessment_year
/*
Data quality observations
- Assessed acres aren't coming in until 2016 for my house, how does this look for other parcels?
  - Is it safe to backfill acres if parcel exists, but value is 0?
- Improvement value and land value look good
- Full property address doesn't come until 2007
  - Safe to backfill this if parcel exists?
  */