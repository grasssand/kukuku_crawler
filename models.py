#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from orm import (
    Model, StringField, BooleanField, IntegerField, TextField, DateField)

class Thread(Model):
    __table__ = 'threads'

    id = IntegerField(primary_key=True)
    uid = StringField(ddl='varchar(20)')
    name = StringField(ddl='varchar(50)')
    email = StringField(ddl='varchar(50)')
    title = StringField(ddl='varchar(200)')
    forum = IntegerField()
    content = TextField()
    image = StringField(ddl='varchar(100)')
    created_at = DateField()
    updated_at = DateField()


class Reply(Model):
    __table__ = 'replys'

    id = IntegerField(primary_key=True)
    parent = IntegerField()
    uid = StringField(ddl='varchar(20)')
    name = StringField(ddl='varchar(50)')
    email = StringField(ddl='varchar(50)')
    title = StringField(ddl='varchar(200)')
    forum = IntegerField()
    content = TextField()
    image = StringField(ddl='varchar(100)')
    created_at = DateField()
    updated_at = DateField()
