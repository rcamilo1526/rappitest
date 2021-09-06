-- 1. members by all the city
create or replace table aux.members_all_city as
with members_city as (select distinct member_id,city
                  from public.members),
     members_city_clean_parts as (select 
                            replace(replace(replace(replace(replace(city,'san francisco','San Francisco'),' Park',''),' Mills',''),' Ridge',''),' Heights','') as city,member_id
                            from members_city),
    members_city_clean_cardinality as (select
                            replace(replace(replace(replace(city,'East ',''),'West ',''),'North ',''),'South ','') as city, member_id
                            from members_city_clean_parts )
select city, count(member_id) as members
from members_city_clean_cardinality
group by city
order by members asc;

-- 2. average rating of groups of Illinois (City of Chicago)
create or replace table aux.avg_rating_chicago as
select city, avg(rating) as avg_rating
from public.groups
where state = 'IL'
group by city
order by avg_rating desc;



--3. insert correlation between duration and tes_rvsp
--create or replace table aux.correlation_duration_yesrsvp as
--select corr(duration,yes_rsvp_count) as correlation, GETDATE() as date
--from events;
insert into aux.correlation_duration_yesrsvp (correlation,date)
select corr(duration,yes_rsvp_count) as correlation, GETDATE() as date
from public.events;
select * from aux.correlation_duration_yesrsvp;

-- 4. table with total members by category with the sum of members in groups of this category note: the group name isn't in correct form that is why I use the name in categories
create or replace table aux.popular_categories as
select c.category_name, sum(g.members) as total_members
from groups g
join categories c on g.category_id = c.category_id
group by c.category_name
order by total_members desc;

select * from aux.popular_categories;

-- 5. table whit the quantity groups growth along the years
create or replace table aux.groups_growth as

with group_year_creation as (select
                    group_id,
                    created,
                    extract (year from created) as year,
                    extract (month from created) as month
                  from
                    public.groups),
   groups_by_date as (select year,month, count(group_id) as groups
                  from group_year_creation
                  group by year,month
                  order by year,month)
select concat(year,'-',month) as month,groups 
from groups_by_date;


select * from aux.groups_growth;
    

-- 6. table with the topics with the number of members and groups if the topic is related to technology in topic_key in table members_topics
create or replace table aux.technology_topics as
with groups_by_topic as (select topic_id, count(group_id) as groups
                        from groups_topics group by topic_id),
     techno_topic_key_ids as (select distinct topic_id from members_topics
                      where topic_key like '%Techno%'
                         or topic_key like '%techno%')
select t.topic_id,t.topic_name,t.members, gt.groups ,t.link
from public.topics t
join groups_by_topic gt 
on t.topic_id = gt.topic_id
where t.topic_id in (select * from techno_topic_key_ids)
order by members desc;

select * from aux.technology_topics;

-- 7. top 15 venues with highest normalised_rating merge the data (test adding 5 tio the top)
create or replace table aux.best_rated_venues as
select venue_id, venue_name,city, normalised_rating, 
                            getdate() as date_in_top
                            from venues 
                            order by normalised_rating desc
                            limit 7;

create or replace table aux.temp_best_rated_venues as
select venue_id, venue_name ,city, normalised_rating,  
                            getdate() as date_in_top
                            from venues 
                            order by normalised_rating desc
                            limit 15;
                    
merge into aux.best_rated_venues as target using aux.temp_best_rated_venues as source
on source.venue_id = target.venue_id
when not matched then
    INSERT (venue_id,venue_name, city,normalised_rating,date_in_top) 
    VALUES (source.venue_id,source.venue_name,source.city,source.normalised_rating,source.date_in_top);
    
drop table aux.temp_best_rated_venues;

select * from aux.best_rated_venues;

-- review updates 
SELECT TABLE_NAME, CREATED, LAST_ALTERED FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND table_schema = 'AUX';