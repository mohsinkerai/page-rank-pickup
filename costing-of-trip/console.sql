select date_add(timestamp, INTERVAL trip_time_seconds SECOND), timestamp from trip where id = 1;

update trip set trip_end_timestamp = date_add(timestamp, INTERVAL trip_time_seconds SECOND) where trip_time_seconds is not null;

select * from trip where taxi_id = 20000001;

select trip_time_seconds, count(*) from trip group by trip_time_seconds;

SELECT t.id, t.lat_pickup, t.lng_pickup, t.lat_dropoff, t.lng_dropoff, t.taxi_id, t.taxi_id, t.timestamp, t.trip_end_timestamp FROM trip t WHERE t.taxi_id in (2004) ORDER BY t.taxi_id, t.timestamp

show variables like 'max_allowed_packet';

SET GLOBAL max_allowed_packet=9073741824;

SELECT count(distinct taxi_id) from trip

show processlist

INSERT INTO results_pickup_pred(fold, taxi_id, followed_prediction, trip_time, bucket) VALUES ()

delete from results_pickup_pred where id = 99999999999999


select distinct  fold from results_pickup_pred where k_means_k = 50

select avg(trip_time), count(*) from results_pickup_pred where k_means_k = 100 and followed_prediction = 1 and bucket <= 3 and experiment = 1

select avg(trip_time), count(*) from results_pickup_pred where k_means_k = 100 and experiment=1 and ((followed_prediction = 1 and bucket > 7) OR followed_prediction = 0)


select fold, avg(trip_time), count(*) from results_pickup_pred where k_means_k = 50 and followed_prediction = 1 and bucket = 2 group by fold

update results_pickup_pred set experiment = 1;

update trip set trip_distance = null;

select  missing_data from trip where id =520
select count(id) from trip where trip_distance is null

select trip_distance, (2+(trip_distance/1000)+(trip_seconds/60)*0.4) from trip where trip_distance is not null and id < 20;

select
  trip_distance,
  trip_time_seconds,
  (2 + (trip_distance / 1000) + (trip_time_seconds/60)*0.4)
from trip
where trip_distance is not null and trip_time_seconds is not null and id < 20;

update trip
set trip_cost = (2 + (trip_distance / 1000) + (trip_time_seconds / 60) * 0.4)
where trip_distance is not null and trip_time_seconds is not null and trip_cost is null;



update trip
set trip_cost = 6
where trip_cost is not null and trip_cost < 6;

use thesis

select distinct  experiment from results_pickup_pred


show processlist

create table results_pickup_pred_copy
(
	id int auto_increment
		primary key,
	k_means_k int null,
	fold int null,
	taxi_id int null,
	followed_prediction int null,
	score double null,
	bucket int null,
	trip_id int null,
	experiment int null,
  cost_earned double null,
  between_trip_time double null,
  trip_time double null
);

select avg(cost_earned/(trip_time+between_trip_time)) from results_pickup_pred_copy where followed_prediction = 1;
select avg(cost_earned/(trip_time+between_trip_time)) from results_pickup_pred_copy where followed_prediction = 0;


select cost_earned, trip_time, between_trip_time from results_pickup_pred_copy where followed_prediction = 0;

use thesis;

select distinct bucket from results_pickup_pred_copy;

select avg(cost_earned), avg(trip_time), avg(between_trip_time), avg(cost_earned/(trip_time/60+between_trip_time/60)) from results_pickup_pred_copy where followed_prediction = 1 and experiment=7 and bucket <= 3;
select avg(cost_earned), avg(trip_time), avg(between_trip_time), avg(cost_earned/(trip_time/60+between_trip_time/60)) from results_pickup_pred_copy where experiment=7 and (followed_prediction = 0 or (followed_prediction = 1 and bucket > 3));

select cost_earned, trip_time, between_trip_time from results_pickup_pred_copy where followed_prediction = 1 and experiment=5;
select cost_earned, trip_time, between_trip_time from results_pickup_pred_copy where followed_prediction = 0 and experiment=5;

select * from trip where trip_cost is null

select dd.hour, avg(trip_cost), avg(trip_distance/1000) from trip t inner join date_dimention dd on dd.id = t.date_dimention_id where t.lat_pickup is not null and dd.is_weekend = true group by dd.hour

select dd.hour, avg(trip_time_seconds/60) from trip t inner join date_dimention dd on dd.id = t.date_dimention_id where t.lat_pickup is not null and dd.is_weekend = true group by dd.hour

select tt.year, tt.month, avg(occupancy) from (select dd.year, dd.month, dd.date, t.taxi_id, sum(trip_time_seconds) as occupancy from trip t inner join date_dimention dd on dd.id = t.date_dimention_id where t.lat_pickup is not null and dd.is_weekend = true group by dd.year, dd.month, dd.date, t.taxi_id) tt group by tt.year, tt.month, tt.date


select distinct is_weekend from date_dimention

select distinct missing_data from trip

select missing_data, count(*) from trip group by  missing_data;

select count(*) from results_pickup_pred_copy where experiment = 5;
select cost_earned, trip_time, between_trip_time from results_pickup_pred_copy where followed_prediction = 1 and experiment=5;
select cost_earned, trip_time, between_trip_time from results_pickup_pred_copy where followed_prediction = 0 and experiment=5;


select id, cost_earned, between_trip_time, taxi_id from results_pickup_pred_copy where followed_prediction = 1 and experiment=7 and bucket <= 3 and fold = 7;

select fold, avg(cost_earned), avg(between_trip_time) from results_pickup_pred_copy where followed_prediction = 1 and experiment=7 and bucket <= 3 group by fold;
select fold, avg(cost_earned), avg(between_trip_time) from results_pickup_pred_copy where experiment=7 and (followed_prediction = 0 or (followed_prediction = 1 and bucket > 3)) group by fold;


select * from trip where polyline = '[]'

select count(*) from trip where lat_pickup is null;
select count(*) from trip where lat_dropoff = lat_pickup;

select count(*) from trip where trip_time_seconds < 76 or lat_pickup is null;

select count(*) from trip;