-- CATEGORIES
create table "MEETUP"."PUBLIC"."CATEGORIES"(
    category_id text,
    category_name text,
    shortname text,
    sort_name text
);

-- select * from categories;
-- drop table "MEETUP"."PUBLIC"."CATEGORIES";
ALTER TABLE categories
ADD PRIMARY KEY (category_id);


-- CITIES
create table "MEETUP"."PUBLIC"."CITIES"(
    city text,
    city_id text,
    country text,
    distance float,
    latitude float,
    localized_country_name text,
    longitude float,
    member_count numeric,
    ranking numeric,
    state text,
    zip text
);

-- select * from cities;
-- drop table "MEETUP"."PUBLIC"."CITIES";
ALTER TABLE cities
ADD PRIMARY KEY (city_id);


-- EVENTS
create table "MEETUP"."PUBLIC"."EVENTS"(
    event_id text,
    created timestamp,
    description text, 
    duration float,
    event_url text,
    fee_accepts text,
    fee_amount float,
    fee_currency text,
    fee_description text,
    fee_label text,
    fee_required boolean,
    group_created timestamp, 
    group_group_lat float,
    group_group_lon float,
    group_id text,
    group_join_mode text,
    group_name text,
    group_urlname text,
    group_who text,
    headcount numeric,
    how_to_find_us text,
    maybe_rsvp_count numeric,
    event_name text,
    photo_url text,
    rating_average float,
    rating_count numeric,
    rsvp_limit numeric,
    event_status text,
    event_time timestamp,
    updated timestamp,
    utc_offset numeric,
    venue_address_1 text,
    venue_address_2 text,
    venue_city text,
    venue_country text,
    venue_id text,
    venue_lat float,
    venue_localized_country_name text,
    venue_lon float,
    venue_name text,
    venue_phone text,
    venue_repinned numeric,
    venue_state text,
    venue_zip text,
    visibility text,
    waitlist_count numeric,
    why text,
    yes_rsvp_count numeric
);

-- select * from events;
-- drop table "MEETUP"."PUBLIC"."EVENTS";
ALTER TABLE events
ADD PRIMARY KEY (event_id);


-- GROUPS
create table "MEETUP"."PUBLIC"."GROUPS"(
    group_id text,
    category_id text,
    category_name text,
    category_shortname text,
    city_id text,
    city text,
    country text,
    created timestamp,
    description text,
    group_photo_base_url text,
    group_photo_highres_link text,
    group_photo_photo_id text,
    group_photo_photo_link text,
    group_photo_thumb_link text,
    group_photo_type text,
    join_mode text,
    lat float,
    link text,
    lon float,
    members numeric,
    group_name text,
    organizer_member_id text,
    organizer_name text,
    organizer_photo_base_url text,
    organizer_photo_highres_link text,
    organizer_photo_photo_id text,
    organizer_photo_photo_link text,
    organizer_photo_thumb_link text,
    organizer_photo_type text,
    rating float,
    state text,
    timezone text,
    urlname text,
    utc_offset numeric,
    visibility text,
    who text
);

-- select * from groups;
-- drop table "MEETUP"."PUBLIC"."GROUPS";
ALTER TABLE groups
ADD PRIMARY KEY (group_id);


-- GROUPS_TOPICS
create table "MEETUP"."PUBLIC"."GROUPS_TOPICS"(
    topic_id text,
	topic_key text,
	topic_name text,
	group_id text
);


-- select * from groups_topics;
-- drop table "MEETUP"."PUBLIC"."GROUPS_TOPICS";
ALTER TABLE groups_topics
ADD PRIMARY KEY (topic_id,group_id);


-- MEMBERS
create table "MEETUP"."PUBLIC"."MEMBERS"(
    member_id text,
	bio text,
	city text,
	country text,
	hometown text,
	joined timestamp,
	lat float,
	link text,
	lon float,
	member_name text,
	state text,
	member_status text,
	visited timestamp,
	group_id text
);

-- select * from members;
-- drop table "MEETUP"."PUBLIC"."MEMBERS";
ALTER TABLE members
ADD PRIMARY KEY (member_id,group_id);



-- MEMBERS_TOPICS
create table "MEETUP"."PUBLIC"."MEMBERS_TOPICS"(
    topic_id text,
	topic_key text,
	topic_name text,
	member_id text
);

-- select * from members_topics;
-- drop table "MEETUP"."PUBLIC"."MEMBERS_TOPICS";
ALTER TABLE members_topics
ADD PRIMARY KEY (member_id,topic_id);


-- TOPICS
create table "MEETUP"."PUBLIC"."TOPICS"(
    topic_id text,
	description text,
	link text,
	members numeric,
	topic_name text,
	urlkey text,
	main_topic_id text
);

-- select * from topics;
-- drop table "MEETUP"."PUBLIC"."TOPICS";
ALTER TABLE topics
ADD PRIMARY KEY (topic_id);


-- VENUES
create table "MEETUP"."PUBLIC"."VENUES"(
    venue_id text,
	address_1 text,
	city text,
	country text,
	distance float,
	lat float,
	localized_country_name text,
	lon float,
	venue_name text,
	rating float,
	rating_count float,
	state text,
	zip text,
	normalised_rating float
);

-- select * from venues;
-- drop table "MEETUP"."PUBLIC"."VENUES";
ALTER TABLE venues
ADD PRIMARY KEY (venue_id);


alter table groups
add foreign key(category_id)
references categories(category_id);

alter table groups
add foreign key(city_id)
references cities(city_id);

alter table groups_topics
add foreign key(group_id)
references groups(group_id);

alter table groups_topics
add foreign key(topic_id)
references topics(topic_id);

alter table members
add foreign key(group_id)
references groups(group_id);

alter table events
add foreign key(group_id)
references groups(group_id);

alter table events
add foreign key(venue_id)
references venues(venue_id);

alter table members_topics
add foreign key(topic_id)
references topics(topic_id);