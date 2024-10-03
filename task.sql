create table users (
    user_id int auto_increment primary key,
    username varchar(50),
    country varchar(100),
    email varchar(50),
    phone varchar(50)
);

create table subscriptions (
    subscription_id int auto_increment primary key,
    user_id int,
    type_of_plan varchar(50),
    subscription_start date,
    subscription_end date,
    foreign key (user_id) references users(user_id)
);

create table movies (
    movie_id int auto_increment primary key,
    title varchar(100),
    year int,
    genre varchar(50),
    duration_minutes int
);

create table watch_history (
    watch_id int auto_increment primary key,
    user_id int,
    movie_id int,
    watch_date date,
    foreign key (user_id) references users(user_id),
    foreign key (movie_id) references movies(movie_id)
);

create index idx_watch_history_date
on watch_history(watch_date);

with cte as (
	select u.username, s.type_of_plan, m.title, wh.watch_date
	from watch_history as wh
	join movies as m on wh.movie_id = m.movie_id
	join subscriptions as s on wh.user_id = s.user_id
	join users as u on wh.user_id = u.user_id
	where watch_date > '2024-01-01'
),
cnt_watched_movies as (
select username, count(*) as cnt
from cte
group by username
)
select
(select concat(username, ": ", cnt) from cnt_watched_movies where cnt = (select min(cnt) as min_cnt from cnt_watched_movies) limit 1) as min_cnt,
(select concat(username, ": ", cnt) from cnt_watched_movies where cnt = (select max(cnt) as max_cnt from cnt_watched_movies) limit 1) as max_cnt;


select
    (select concat(username, ": ", cnt)
     from (select username, count(*) as cnt
           from (select u.username, s.type_of_plan, m.title, wh.watch_date
                 from watch_history as wh
                 join movies as m on wh.movie_id = m.movie_id
                 join subscriptions as s on wh.user_id = s.user_id
                 join users as u on wh.user_id = u.user_id
                 where wh.watch_date > '2024-01-01') as sub1
           group by username) as sub2
     where cnt = (select min(cnt)
                  from (select count(*) as cnt
                        from (select u.username, s.type_of_plan, m.title, wh.watch_date
                              from watch_history as wh
                              join movies as m on wh.movie_id = m.movie_id
                              join subscriptions as s on wh.user_id = s.user_id
                              join users as u on wh.user_id = u.user_id
                              where wh.watch_date > '2024-01-01') as sub3
                        group by username) as sub4)
     limit 1) as min_cnt,

    (select concat(username, ": ", cnt)
     from (select username, count(*) as cnt
           from (select u.username, s.type_of_plan, m.title, wh.watch_date
                 from watch_history as wh
                 join movies as m on wh.movie_id = m.movie_id
                 join subscriptions as s on wh.user_id = s.user_id
                 join users as u on wh.user_id = u.user_id
                 where wh.watch_date > '2024-01-01') as sub1
           group by username) as sub2
     where cnt = (select max(cnt)
                  from (select count(*) as cnt
                        from (select u.username, s.type_of_plan, m.title, wh.watch_date
                              from watch_history as wh
                              join movies as m on wh.movie_id = m.movie_id
                              join subscriptions as s on wh.user_id = s.user_id
                              join users as u on wh.user_id = u.user_id
                              where wh.watch_date > '2024-01-01') as sub3
                        group by username) as sub4)
     limit 1) as max_cnt;