DROP TABLE IF EXISTS immigration
DROP TABLE IF EXISTS temperature
DROP TABLE IF EXISTS demographics
DROP TABLE IF EXISTS airports


CREATE TABLE public.immigration (
	ID int,
	year int,
	month int,
	native_country varchar,
	residence_country varchar,
	port_of_admission varchar,
	arrival_state varchar,
	arrival_date date,
	departure_date date,
	visa varchar,
	birth_city varchar,
	birth_year int,
	gender varchar,
	airline varchar,
	Mode varchar,
	visatype varchar,
	CONSTRAINT immigration_pkey PRIMARY KEY ("ID")
);

CREATE TABLE public.temperature (
	Country varchar,
	Temperature float,
	Latitude varchar,
	Longitude varchar,
	CONSTRAINT temperature_pkey PRIMARY KEY ("Country")
);

CREATE TABLE public.demographics (
	City varchar,
	State varchar,
	Median_age varchar,
	Male_population int,
	Female_population int,
	Total_population int,
	Number_veterans int,
	Foreign_born int,
	Average_household_size varchar,
	State_code varchar,
	Race varchar,
	Count int,
	CONSTRAINT demographics_pkey PRIMARY KEY ("state_code")
);

CREATE TABLE public.airports (
	ident varchar,
	type varchar,
	name varchar,
	elevation_ft varchar,
	continent varchar,
	iso_country varchar,
	state varchar,
	municipality varchar,
	gps_code varchar,
	iata_code varchar,
	local_code varchar,
	coordinates varchar,
	CONSTRAINT airport_pkey PRIMARY KEY ("state")
) ;
