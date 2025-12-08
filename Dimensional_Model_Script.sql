
----Rodrigo Araújo----

--select * from loadsmart_dim.dim_pickup_location ;
--select * from loadsmart_dim.dim_delivery_location;
--select * from loadsmart_dim.dim_sourcing_channel;
--select * from loadsmart_dim.dim_carrier;
--select * from loadsmart_dim.dim_shipper;
--
--select * from loadsmart_fact.fact_loadsmart_load ;
--
--select * from loadsmart_anl.anl_pickup_location;
--select * from loadsmart_anl.anl_delivery_location;
--select * from loadsmart_anl.anl_carrier;
--select * from loadsmart_reports.loadsmart_id_lastmonth_report;



-----RAW TABLE:
--select * from loadsmart."2025_data_challenge_ae" limit 100 --5454 port.


-------------------------------------------------------------------------------------------------------------------
---Cleanning our env:
drop table if exists loadsmart_fact.fact_loadsmart_load ;
----
drop table if exists loadsmart_dim.dim_pickup_location;
drop table if exists loadsmart_dim.dim_delivery_location;
drop table if exists loadsmart_dim.dim_sourcing_channel;
drop table if exists loadsmart_dim.dim_carrier;
drop table if exists loadsmart_dim.dim_shipper;
-----
drop table if exists loadsmart_anl.anl_pickup_location;
drop table if exists loadsmart_anl.anl_delivery_location;
drop table if exists loadsmart_anl.anl_carrier;
----
drop table if exists loadsmart_reports.loadsmart_id_lastmonth_report ;
-------------------------------------------------------------------------------------------------------------------


-------------------------------------------------------------------------------------------------------------------
---All Schemas that we need created:
CREATE schema if not exists loadsmart_dim;
CREATE schema if not exists loadsmart_fact;
CREATE schema if not exists loadsmart_anl;
CREATE schema if not exists loadsmart_reports;
-------------------------------------------------------------------------------------------------------------------




--------------------------------------------------------------------------------------------------------------------
---Part: 2.a.i ---I'll need to transform this in some function not only a dimension
---CITIES and STATES:

CREATE TABLE IF NOT EXISTS loadsmart_dim.dim_pickup_location (
    pickup_id SERIAL PRIMARY KEY,
    pickup_city TEXT,
    pickup_state TEXT,
    line_created_at timestamp default current_timestamp,
    line_updated_at timestamp default current_timestamp,
    UNIQUE(pickup_city, pickup_state)
);

insert into loadsmart_dim.dim_pickup_location (pickup_id, pickup_city, pickup_state) values (-1, 'Unknown', 'Unknown') on conflict(pickup_id) do nothing ;


merge into loadsmart_dim.dim_pickup_location target
using
	(
	SELECT DISTINCT
	    pickup_city,
	    pickup_state
	FROM loadsmart."2025_data_challenge_ae"
		WHERE lane IS NOT NULL	 
	) 
		as ssource 	on 	target.pickup_city = ssource.pickup_city 
					and target.pickup_state = ssource.pickup_state
when not matched then 
insert 
(
	 pickup_city
	,pickup_state
)
values
(
	 ssource.pickup_city
	,ssource.pickup_state
)	
;


CREATE TABLE IF NOT EXISTS loadsmart_dim.dim_delivery_location (
    delivery_id SERIAL PRIMARY KEY,
    delivery_city TEXT,
    delivery_state TEXT,
    line_created_at timestamp default current_timestamp,
    line_updated_at timestamp default current_timestamp,
    UNIQUE(delivery_city, delivery_state)
);

insert into loadsmart_dim.dim_delivery_location (delivery_id, delivery_city, delivery_state) values (-1, 'Unknown', 'Unknown') ON CONFLICT (delivery_id) DO NOTHING;


merge into loadsmart_dim.dim_delivery_location target
using
	(
	SELECT DISTINCT
	    delivery_city,
	    delivery_state
	FROM loadsmart."2025_data_challenge_ae"
		WHERE lane IS NOT NULL	 
	) 
		as ssource 	on 	target.delivery_city = ssource.delivery_city 
					and target.delivery_state = ssource.delivery_state
when not matched then 
insert 
(
	 delivery_city
	,delivery_state
)
values
(
	 ssource.delivery_city
	,ssource.delivery_state
)	
;



--------------------------------------------------------------------------------------------------------------------
----DIM_SOURCING_CHANNEL:

CREATE TABLE IF NOT EXISTS loadsmart_dim.dim_sourcing_channel (
    sourcing_channel_id SERIAL PRIMARY key,    
    sourcing_channel TEXT,
    line_created_at timestamp default current_timestamp,
    line_updated_at timestamp default current_timestamp,
    UNIQUE(sourcing_channel)
);

INSERT INTO loadsmart_dim.dim_sourcing_channel ( sourcing_channel_id, sourcing_channel) VALUES (-1, 'Unknown') ON CONFLICT (sourcing_channel_id) DO nothing ;


merge into loadsmart_dim.dim_sourcing_channel target
using
	(
	SELECT DISTINCT
	    sourcing_channel
	FROM loadsmart."2025_data_challenge_ae"
		WHERE sourcing_channel IS NOT NULL	 
	) 
		as ssource 	on 	target.sourcing_channel = ssource.sourcing_channel					
when not matched then 
insert 
(
	 sourcing_channel	
)
values
(	 
	 ssource.sourcing_channel
)	
;



-------------------------------------------------------------------------------------

---DIM Carrier:
CREATE TABLE IF NOT exists  loadsmart_dim.dim_carrier (
    carrier_id SERIAL PRIMARY KEY,
    carrier_name TEXT,
    carrier_segment TEXT,
    line_created_at timestamp default current_timestamp,
    line_updated_at timestamp default current_timestamp,
    UNIQUE(carrier_name, carrier_segment)
);

insert into loadsmart_dim.dim_carrier (carrier_id, carrier_name, carrier_segment) values (-1, 'Unknown', 'Unknown') on conflict(carrier_id) do nothing;

merge into loadsmart_dim.dim_carrier as target 
using (select distinct carrier_name, 'transportation' as carrier_segment from loadsmart."2025_data_challenge_ae" where carrier_name is not null ) as ssource
	on target.carrier_name = ssource.carrier_name
when not matched then 
insert 
(
	 carrier_name
	,carrier_segment
)
values
(
	 ssource.carrier_name
	,ssource.carrier_segment
);



--------------------------------------------------------------------------------------------------------------------

---DIM Shipper:
CREATE TABLE IF NOT exists  loadsmart_dim.dim_shipper (
    shipper_id SERIAL PRIMARY KEY,
    shipper_name TEXT,    
    line_created_at timestamp default current_timestamp,
    line_updated_at timestamp default current_timestamp,
    UNIQUE(shipper_name)
);

insert into loadsmart_dim.dim_shipper (shipper_id, shipper_name) values (-1, 'Unknown') on conflict(shipper_id) do nothing;

merge into loadsmart_dim.dim_shipper as target 
using (select distinct shipper_name from loadsmart."2025_data_challenge_ae" where shipper_name is not null ) as ssource
	on target.shipper_name = ssource.shipper_name
when not matched then 
insert 
(
	 shipper_name	
)
values
(
	 ssource.shipper_name
);


--------------------------------------------------------------------------------------------------------------------

-------- FACT TABLE CHECK CREATE----------
CREATE TABLE IF NOT EXISTS loadsmart_fact.fact_loadsmart_load (
    loadsmart_id BIGINT PRIMARY KEY,
	--    
    pickup_id INT REFERENCES loadsmart_dim.dim_pickup_location(pickup_id),
    delivery_id INT REFERENCES loadsmart_dim.dim_delivery_location(delivery_id),
	--
    shipper_id int REFERENCES loadsmart_dim.dim_shipper(shipper_id),
    carrier_id INT REFERENCES loadsmart_dim.dim_carrier(carrier_id),    	
    --
    sourcing_channel_id int, --references loadsmart_dim.dim_sourcing_channel(sourcing_channel_id),
	--
    vip_carrier boolean default false,    
	quote_date                 timestamp default '1900-01-01',
	book_date                  timestamp default '1900-01-01',
	source_date                timestamp default '1900-01-01',
	pickup_date                timestamp default '1900-01-01',
	delivery_date              timestamp default '1900-01-01',
	pickup_appointment_time    timestamp default '1900-01-01',
	delivery_appointment_time  timestamp default '1900-01-01',
	--
    book_price NUMERIC,
    source_price NUMERIC,
    pnl NUMERIC,
    mileage numeric,   
    equipment_type text,
    carrier_rating smallint,
	--
    carrier_dropped_us_count int default 0,  -- I'd like to understand this field better. Is the result of the line for each loadsmart_id of the carrier or a cumulative value during the carrier life?
    carrier_on_time_to_pickup boolean default false, 
    carrier_on_time_to_delivery boolean default false,
    carrier_on_time_overall boolean default false, 
    contracted_load boolean default false,
    load_booked_autonomously boolean default false,
    load_sourced_autonomously boolean default false,
    load_was_cancelled boolean default false,
    line_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_loadsmart_id			ON loadsmart_fact.fact_loadsmart_load USING brin (loadsmart_id);
CREATE INDEX IF NOT EXISTS idx_pickup_id			ON loadsmart_fact.fact_loadsmart_load USING brin (pickup_id);
CREATE INDEX IF NOT EXISTS idx_delivery_id			ON loadsmart_fact.fact_loadsmart_load USING brin (delivery_id);
CREATE INDEX IF NOT EXISTS idx_shipper_id			ON loadsmart_fact.fact_loadsmart_load USING brin (shipper_id);
CREATE INDEX IF NOT EXISTS idx_carrier_id			ON loadsmart_fact.fact_loadsmart_load USING brin (carrier_id);
CREATE INDEX IF NOT EXISTS idx_sourcing_channel_id	ON loadsmart_fact.fact_loadsmart_load USING brin (sourcing_channel_id);
CREATE INDEX IF NOT EXISTS idx_line_created_at		ON loadsmart_fact.fact_loadsmart_load USING brin (line_created_at);



---In my use_case I used the concept of Immutable Fact Table and only INSERTs can be possible:
---I used a simple incremental concept for this case using a Timestamp comparison -->>> ingestion_date > line_created_at
WITH dedup AS (
    SELECT
        raw.*,        
        ROW_NUMBER() OVER ( PARTITION BY raw.loadsmart_id  ORDER BY raw.delivery_date::timestamp DESC  ) AS rn
    FROM loadsmart."2025_data_challenge_ae" raw
    	where raw.ingestion_date > ( select coalesce( max(line_created_at) , '1900-01-01'::date) from loadsmart_fact.fact_loadsmart_load )
)
INSERT INTO loadsmart_fact.fact_loadsmart_load 
(
    loadsmart_id,
    pickup_id,
    delivery_id,
    shipper_id,
    carrier_id,
    sourcing_channel_id,
    vip_carrier,
    quote_date,
    book_date,
    source_date,
    pickup_date,
    delivery_date,
	pickup_appointment_time,
	delivery_appointment_time,    
    book_price,
    source_price,
    pnl,
    mileage,
    equipment_type,
    carrier_rating,
	carrier_dropped_us_count,
	carrier_on_time_to_pickup,
	carrier_on_time_to_delivery,
    carrier_on_time_overall,	
    contracted_load,
    load_booked_autonomously,
    load_sourced_autonomously,
    load_was_cancelled,    
    line_created_at 
)	
SELECT
    d.loadsmart_id,
    --    
    coalesce(pl.pickup_id, -1) 			as pickup_id,
    coalesce(dl.delivery_id, -1) 		as delivery_id,
    coalesce(dsh.shipper_id, -1) 		as shipper_id,
    coalesce(dcar.carrier_id, -1) 		as carrier_id,
    coalesce(sourcing_channel_id, -1) 	as sourcing_channel_id,    
	--
	d.vip_carrier,
	--
	coalesce( cast(d.quote_date as timestamp), '1900-01-01' )                     as quote_date,
	coalesce( cast(d.book_date as timestamp), '1900-01-01' )                      as book_date,
	coalesce( cast(d.source_date as timestamp), '1900-01-01' )                    as source_date,
	coalesce( cast(d.pickup_date as timestamp), '1900-01-01' )                    as pickup_date,
	coalesce( cast(d.delivery_date as timestamp), '1900-01-01' )                  as delivery_date,
	coalesce( cast(d.pickup_appointment_time as timestamp), '1900-01-01' )        as pickup_appointment_time,
	coalesce( cast(d.delivery_appointment_time as timestamp), '1900-01-01' )      as delivery_appointment_time,
	--
    d.book_price,
    d.source_price,
    d.pnl,   --PROFIT or LOSS based in Book(client) and Source(us) Price
    d.mileage,
    d.equipment_type,
    coalesce(d.carrier_rating, 0) as carrier_rating,
    d.carrier_dropped_us_count,
    case when lower(d.carrier_on_time_to_pickup) = 'true' then true else false end as carrier_on_time_to_pickup,
    case when lower(d.carrier_on_time_to_delivery) = 'true' then true else false end as carrier_on_time_to_delivery,
    case when lower(d.carrier_on_time_overall) = 'true' then true else false end as carrier_on_time_overall,
    d.contracted_load,
    d.load_booked_autonomously,
    d.load_sourced_autonomously,
    d.load_was_cancelled,    
    d.ingestion_date as line_created_at
	--    
FROM dedup d
-- Dim Locations
left join loadsmart_dim.dim_pickup_location pl 
		on d.pickup_city = pl.pickup_city and d.pickup_state = pl.pickup_state
left join loadsmart_dim.dim_delivery_location dl 
		on d.delivery_city = dl.delivery_city and d.delivery_state = dl.delivery_state		
-- dim carrier
left join loadsmart_dim.dim_carrier dcar
    on dcar.carrier_name = d.carrier_name
left join loadsmart_dim.dim_shipper dsh
	on dsh.shipper_name = d.shipper_name
left join loadsmart_dim.dim_sourcing_channel sc 
	on d.sourcing_channel = sc.sourcing_channel
	where d.rn = 1	
--
	on conflict(loadsmart_id) do nothing	
;




------------------------------------------------------------------------------------------------------------------------

--STAR pickup_location PNL analysis:
create table loadsmart_anl.anl_pickup_location as (
	select 
		 f.pickup_id
		,pl.pickup_city 
		,pl.pickup_state 
		,cast(f.delivery_date as date) as delivery_date
		,sum(f.book_price) as sum_book_price
		,sum(f.source_price) as sum_source_price
		,sum(f.pnl) as sum_pnl
	from loadsmart_fact.fact_loadsmart_load f
		left join loadsmart_dim.dim_pickup_location pl on f.pickup_id = pl.pickup_id
	group by 1,2,3,4	
--		order by pickup_id, delivery_date
);


--STAR delivery_location PNL analysis:
create table loadsmart_anl.anl_delivery_location as (
	select 
		 f.delivery_id
		,pl.delivery_city 
		,pl.delivery_state 
		,cast(f.delivery_date as date) as delivery_date
		--
		,sum(f.book_price) as sum_book_price
		,sum(f.source_price) as sum_source_price
		,sum(f.pnl) as sum_pnl
	from loadsmart_fact.fact_loadsmart_load f
		left join loadsmart_dim.dim_delivery_location pl on f.delivery_id = pl.delivery_id
	group by 1,2,3,4	
--		order by pickup_id, delivery_date
);



--STAR carrier analysis:
create table loadsmart_anl.anl_carrier as ( 	
	select 
		 f.carrier_id
		,dc.carrier_name  
		,cast(f.delivery_date as date) as delivery_date
		--
		,avg(carrier_rating) as avg_carrier_rating
		,sum(f.book_price) as sum_book_price
		,sum(f.source_price) as sum_source_price
		,sum(f.pnl) as sum_pnl
		,sum(f.carrier_dropped_us_count) as sum_carrier_dropped_us_count	
	from loadsmart_fact.fact_loadsmart_load f 
		left join loadsmart_dim.dim_carrier dc on f.carrier_id = dc.carrier_id 
	where carrier_name != 'Unknown'	
		group by 1,2,3
	--	order by carrier_id, delivery_date
);
	


--2.b task
create table loadsmart_reports.loadsmart_id_lastmonth_report as ( 
	select 
		 r.loadsmart_id 
		,r.shipper_name 
		,cast(r.delivery_date as timestamp) as delivery_date
		,r.pickup_city 
		,r.pickup_state 
		,r.delivery_city 
		,r.delivery_state 
		,r.book_price 
		,r.carrier_name 		
	from loadsmart."2025_data_challenge_ae" r 
		where 
			cast(r.delivery_date as timestamp) >=
				case 
					when (select count(1) from loadsmart."2025_data_challenge_ae" r where cast(r.delivery_date as timestamp) >= current_date - interval '1' month) > 0
					then (select cast(r.delivery_date as timestamp) from loadsmart."2025_data_challenge_ae" r where cast(r.delivery_date as timestamp) >= current_date - interval '1' month)
					else (select max(cast(r.delivery_date as timestamp)) - interval '3' month from loadsmart."2025_data_challenge_ae" r)
					end
);

			
	
	

