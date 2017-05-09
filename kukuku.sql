drop table threads;
create table threads (
    id integer,
    uid varchar(50),
    name varchar(50),
    email varchar(50),
    title varchar(200),
    forum smallint,
    content text,
    image varchar(100),
    created_at timestamp,
    updated_at timestamp,
    primary key (id)
);

drop table replys;
create table replys (
    id integer,
    parent integer,
    uid varchar(50),
    name varchar(50),
    email varchar(50),
    title varchar(200),
    forum smallint,
    content text,
    image varchar(100),
    created_at timestamp,
    updated_at timestamp,
    primary key (id)
);
