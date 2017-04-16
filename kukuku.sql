drop table threads;
create table threads (
    id integer,
    uid character varying(20),
    name character varying(20),
    email character varying(20),
    title character varying(50),
    forum integer,
    content text,
    image character varying(50),
    created_at timestamp without time zone,
    primary key (id)
);

drop table replys;
create table replys (
    id integer,
    parent integer,
    uid character varying(20),
    name character varying(20),
    email character varying(20),
    title character varying(50),
    forum integer,
    content text,
    image character varying(50),
    created_at timestamp without time zone,
    primary key (id)
);