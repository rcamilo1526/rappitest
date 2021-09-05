-- //correlation between duration and tes_Rvsp
create or replace table aux.correlation_duration_yesrsvp as
select corr(duration,yes_rsvp_count) as correlation
from events;
select  * from aux.correlation_duration_yesrsvp;

-- // table with total members by category with the sum of members in groups of this category note: the group name isn't in correct form
create or replace table aux.popular_categories as
select c.category_name, sum(g.members) as total_members
from groups g
join categories c on g.category_id = c.category_id
group by c.category_name
order by total_members desc;

select * from aux.popular_categories;

-- // quantity groups growth along the years
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
    
select * from members limit 50;

-- // members by city
select count(distinct member_id)
from members;
group by city;
