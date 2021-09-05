--all tables quick view

select * from categories limit 50;
--for categories we have name for categories can joinable by the category_id


select * from cities limit 50;
-- cities we have the location, city is the different parts of the cities, member_count, state, zip, ranking 


select * from events limit 50;
-- events we have the created date of the id, a long description, duration of the event, url
--, fee related vars like accepts: like type of payment [cash, paypal, wepay,others], fee amount [max=95, min=0]
-- fee currency only not_found and usd, fee description: only per person, fee label: only 'price' or 'Price' and if the feed is required
-- related with groups, group creation, latitud, longitud, id, type of join of the group:[open,approval], the group name, urlname, 
--groups who it's like type of the members, headcount is always 0, how_to find_us, maybe_rsvp_count always 0
-- event_name, photo_url, rating_average always 0,rating_count always -1, rsvp_limit min -1 max 200, event_status always upcoming
-- event_time timestamp of the event, updated when the event was updated, utc_offset offset of the utc,  
-- venue vars; addres, adress 2, city, country,id, lat, long,localization, name,phone, repinned,state,zip, 
-- visibility of the event always public, waitlist_count min 0 max 15, why always not found, yes_rsvp_count min 0 max 198


select * from groups limit 50;
-- group_id, category_id, category name, category, shortname, city_id,city,county, created, 
-- description a long description, some vars with group_photo 
--including the group_photo_type [event,other], join_mode[ open ,approval, closed],
--lat, long, link, 
--members, group name, organizer member_id, organizer name, photo,
--state, timezone, urlname, offset_utc, visibility [public, public_limited, members]
-- who it's like type of the members
select count(distinct group_id) from groups;
-- 16330
select count(*) from groups;
-- 16330



select * from groups_topics limit 50;
-- topic by group, topic_name, group_id, topic_id
select count(distinct group_id) from groups_topics;
-- 11876


select * from members limit 50;
-- members with data but the users 
--there are members repeated by group, if a user is in 4 groups the user have 4 registers
-- country always us, hometown like other place,lat, long, link, name, stat
--member status [active, prereg], visited, group_id 
select count(distinct member_id) from members;
-- 1087923
select count(*) from members;
-- 5893886


select * from members_topics  limit 50;
-- topics by members, each register is a topic of the users
select count(distinct member_id) from members_topics;
-- 605790
select count(*) from members_topics;
-- 3195245


select * from topics limit 50;
-- topics with description and link by users, and urlkey, topic name
select count(distinct topic_id) from topics;
-- 2509
select count(*) from topics;
-- 2509



select * from venues limit 50;
-- venues (sedes) with all the data, rating, rating count
select count(distinct venue_id) from venues ;
-- 107093
select count(*) from venues ;
-- 107093
