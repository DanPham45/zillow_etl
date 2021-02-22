import configparser
# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

heating_drop = "DROP TABLE IF EXISTS heating_or_system_type;"
property_land_drop = "DROP TABLE IF EXISTS property_land_use_type;"
story_drop = "DROP TABLE IF EXISTS story_type;"
air_conditioning_drop = "DROP TABLE IF EXISTS air_conditioning_type;"
architecture_style_drop = "DROP TABLE IF EXISTS architectural_style_type;"
type_construction_drop = "DROP TABLE IF EXISTS type_construction_type;"
building_class_drop = "DROP TABLE IF EXISTS building_class_type;"

transactions_drop = "DROP TABLE IF EXISTS property_transactions;"
details_drop = "DROP TABLE IF EXISTS property_details;"
analytics_drop = "DROP TABLE IF EXISTS transactions_by_month;"

# CREATE TABLES

heating_create = ("""
CREATE TABLE heating_or_system_type (
	id	integer	not null,
	heating_or_system 	varchar(30)	not null
	)
diststyle all;
""")

property_land_create = ("""
CREATE TABLE property_land_use_type (
	id	integer	not null,
	property_land_use 	varchar(100)	not null
	)
diststyle all;
""")

story_create = ("""
CREATE TABLE story_type (
	id	integer	not null,
	story 	varchar(100)	not null
	)
diststyle all;
""")

air_conditioning_create = ("""
CREATE TABLE air_conditioning_type (
	id	integer	not null,
	air_conditioning 	varchar(30)	not null
	)
diststyle all;
""")

architecture_style_create = ("""
CREATE TABLE architectural_style_type (
	id	integer	not null,
	architectural_style 	varchar(30)	not null
	)
diststyle all;
""")

type_construction_create = ("""
CREATE TABLE type_construction_type (
	id	integer	not null,
	type_construction 	varchar(30)	not null
	)
diststyle all;
""")

building_class_create = ("""
CREATE TABLE building_class_type (
	id	integer	not null,
	building_class 	varchar(300)	not null
	)
diststyle all;
""")

transactions_create = ("""
CREATE TABLE property_transactions (
	parcel_id	bigint	not null,
	log_error 	float	not null,
	transaction_date	date	not null
	)
diststyle all;
""")

details_create = ("""
CREATE TABLE property_details (
	parcel_id	bigint not null,
	air_conditioning_type_id	int,
	architectural_style_type_id 	int,
	bathroom_count	int,
	bedroom_count	int,
	building_class_type_id	int,
	calculated_finished_square_feet int,
	heating_or_system_type_id int,
	property_land_use_type_id int,
	region_id_city int,
	region_id_county int,
	region_id_zip int,
	room_count int,
	story_type_id int, 
	type_construction_type_id int,
	year_built int,
	tax_value_dollar_count bigint
	)
diststyle even;
""")

analytics_create = ("""
CREATE TABLE transactions_by_month (
	region_id_county	int DISTKEY,
	year	int	not null,
	month	int	not null,
	transaction_count	bigint	not null,
	transaction_value	float
	);
""")

# ANALYTICS QUERY
transactions_by_month = ("""
SELECT d.region_id_county,
extract(year from t.transaction_date) as year,
extract(month from t.transaction_date) as month,
count(distinct t.parcel_id) as transaction_count,
sum(power(2.71828,(ln(cast(d.tax_value_dollar_count as float))+t.log_error))) as transaction_value
FROM transactions t 
LEFT JOIN details d 
ON t.parcel_id = d.parcel_id
group by d.region_id_county,
extract(year from t.transaction_date),
extract(month from t.transaction_date)
""")


# COPY TO REDSHIFT
transactions_copy = """
    copy property_transactions 
    from 's3://zillowanalytics/transactions/'
    iam_role '{}'
    format as parquet;
""".format(config.get("DWH","DWH_ROLE_ARN"))

details_copy = """
    copy property_details 
    from 's3://zillowanalytics/propdetails/'
    iam_role '{}'
    format as parquet;
""".format(config.get("DWH","DWH_ROLE_ARN"))

analytics_copy = """
    copy transactions_by_month 
    from 's3://zillowanalytics/transactions_by_month/'
    iam_role '{}'
    format as parquet;
""".format(config.get("DWH","DWH_ROLE_ARN"))

# CLEAN DATA
drop_detail_columns = ['basementsqft','buildingqualitytypeid','calculatedbathnbr','decktypeid',
                       'finishedfloor1squarefeet', 'finishedsquarefeet12','finishedsquarefeet13','finishedsquarefeet15',
                       'finishedsquarefeet50','finishedsquarefeet6','fips','fireplacecnt','fullbathcnt',
                       'garagecarcnt','garagetotalsqft','hashottuborspa','latitude','longitude',
                       'lotsizesquarefeet','poolcnt','poolsizesum','pooltypeid10','pooltypeid2',
                       'pooltypeid7','propertycountylandusecode','propertyzoningdesc','rawcensustractandblock',
                       'regionidneighborhood','threequarterbathnbr', 'unitcnt','yardbuildingsqft17',
                       'yardbuildingsqft26','numberofstories','fireplaceflag','structuretaxvaluedollarcnt',
                       'assessmentyear','landtaxvaluedollarcnt', 'taxamount','taxdelinquencyflag',
                       'taxdelinquencyyear','censustractandblock']
# QUERY LISTS
drop_dimension_queries = [heating_drop, property_land_drop, story_drop, 
					air_conditioning_drop, architecture_style_drop, 
					type_construction_drop, building_class_drop]

create_dimension_queries = [heating_create, property_land_create, story_create, 
						air_conditioning_create, architecture_style_create, 
						type_construction_create, building_class_create]

drop_fact_queries = [transactions_drop, details_drop, analytics_drop]
create_fact_queries = [transactions_create, details_create, analytics_create]

