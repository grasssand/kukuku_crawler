drop table threads;
create table threads (
    id integer,
    uid character varying(20),
    name character varying(50),
    email character varying(50),
    title character varying(200),
    forum integer,
    content text,
    image character varying(100),
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    primary key (id)
);

drop table replys;
create table replys (
    id integer,
    parent integer,
    uid character varying(20),
    name character varying(50),
    email character varying(50),
    title character varying(200),
    forum integer,
    content text,
    image character varying(100),
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    primary key (id)
);