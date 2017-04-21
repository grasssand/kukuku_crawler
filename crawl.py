#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import logging
import os
import sys
import time
import urllib.parse
from asyncio import Queue
from datetime import datetime

import aiohttp

import orm
from models import Thread, Reply

THREADS_URL = 'http://h.koukuko.com/api/{}?page={}'
REPLYS_URL = 'http://h.koukuko.com/api/t/{}?page={}'
IMAGE_URL = 'http://static.kukuku.cc/{}'
FORUM_TYPE = ['综合版1', ]
MAX_PAGES = 1000
IMAGE_FOLDER = os.path.join(sys.path[0], 'static')
DB_SETTING = {
    'user': 'postgres',
    'password': '8523',
    'database': 'kukuku'
}

LOGGER = logging.getLogger(__name__)

if not os.path.exists(IMAGE_FOLDER):
    os.mkdir(IMAGE_FOLDER)

def is_redirect(response):
    return response.status in (300, 301, 302, 303, 307)

def timestamp2datetime(ts):
    return datetime.fromtimestamp(ts / 1000)


class Crawler:

    def __init__(self, max_redirect=10, max_tries=4, 
                 max_tasks=10, *, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.max_redirect = max_redirect
        self.max_tries = max_tries
        self.max_tasks = max_tasks
        self.q = Queue(loop=self.loop)
        self.seen_urls = set()
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.last_created = None
        self.last_updated = None
        self._stopped = False
        for t in FORUM_TYPE:
            self.add_url(THREADS_URL.format(t, 1))
        self.t0 = time.time()
        self.t1 = None

    def close(self):
        self.session.close()

    async def image_download(self, image):
        img_name = image.split('/')[-1]
        img_url = IMAGE_URL.format(img_name)
        img_path = os.path.join(IMAGE_FOLDER, img_name)
        if not os.path.exists(img_path):
            async with self.session.get(img_url) as response:
                content = await response.read()
                with open(img_path, 'wb') as f:
                    f.write(content)
        return img_name

    async def save_data(self, item):
        item['created_at'] = timestamp2datetime(item['createdAt'])
        item['updated_at'] = timestamp2datetime(item['updatedAt'])
        if item.get('parent'):
            r = Reply(**item)
        else:
            r = Thread(**item)
        try:
            await r.save()
        except Exception as e:
            LOGGER.error('save database error: %r', e)

    async def update_data(self, item):
        item['created_at'] = timestamp2datetime(item['createdAt'])
        item['updated_at'] = timestamp2datetime(item['updatedAt'])
        r = Thread(**item)
        await r.update()

    async def parse_thread_link(self, response):
        '''抓取串'''
        links = set()
        next_url = None
        body = await response.json()
        if response.status == 200:
            for thread in body['data']['threads']:
                # 排除置顶串
                thread_id = thread['id']
                if thread_id == 6960723:
                    continue
                # 断点续爬
                updated_at = timestamp2datetime(thread['updatedAt'])
                if (self.last_updated and 
                        self.last_updated.getValue('updated_at') >= updated_at):
                    self._stopped = True
                    break

                # 串内回复url
                pages = (thread['replyCount'] + 19) // 20
                urls = [REPLYS_URL.format(thread['id'], i)
                        for i in range(pages, 0, -1)]
                if urls:
                    LOGGER.info('got %r urls from %r', 
                                len(urls), response.url)
                links.update(urls)

                # 判断新串
                created_at = timestamp2datetime(thread['createdAt'])
                if (self.last_created is None or
                        self.last_created.getValue('created_at') < created_at):
                    await self.save_data(thread)
                else:
                    await self.update_data(thread)

            size = body['page']['size']
            location = body['page']['page']
            url = response.url.with_query(None)
            if (location < min(size, MAX_PAGES) 
                    if isinstance(MAX_PAGES, int) else size):
                next_url = '{}?page={}'.format(url, location + 1)

        return links, next_url

    async def parse_reply_link(self, response):
        '''抓取回复'''
        body = await response.json()

        if response.status == 200:
            for reply in body['replys']:
                created_at = timestamp2datetime(reply['createdAt'])
                if (self.last_updated is None or
                        self.last_updated.getValue('updated_at') < created_at):
                    await self.save_data(reply)

    async def fetch(self, url, max_redirect):
        print('fetch url: %s' % url)
        tries = 0
        exception = None
        while tries < self.max_tries:
            try:
                response = await self.session.get(url, allow_redirects=False)
                break
            except aiohttp.ClientError as client_error:
                LOGGER.info('try %r for %r raised %r', 
                            tries, url, client_error)
                exception = client_error
            tries += 1
        else:
            LOGGER.error('%r failed after %r tries', url, self.max_redirect)
            return

        try:
            if is_redirect(response):
                location = response.headers['location']
                redirect_url = urllib.parse.urljoin(url, location)
                if redirect_url in self.seen_urls:
                    return
                if max_redirect > 0:
                    LOGGER.info('redirect to %r from %r', redirect_url, url)
                else:
                    LOGGER.error('redirect limit reached from %r from %r', 
                                 redirect_url, url)
            else:
                if 'h.koukuko.com/api/t/' not in url:
                    links, next_url = await self.parse_thread_link(response)
                    if next_url in self.seen_urls:
                        return
                    if next_url is not None:
                        self.add_url(next_url)
                    for link in links.difference(self.seen_urls):
                        self.q.put_nowait((link, self.max_redirect))
                    self.seen_urls.update(links)
                else:
                    await self.parse_reply_link(response)
        finally:
            await response.release()

    async def work(self):
        try:
            while True:
                url, max_redirect = await self.q.get()
                assert url in self.seen_urls
                await self.fetch(url, max_redirect)
                self.q.task_done()
                asyncio.sleep(2)
        except asyncio.CancelledError:
            pass

    def add_url(self, url, max_redirect=None):
        if max_redirect is None:
            max_redirect = self.max_redirect
        if not self._stopped:
            self.seen_urls.add(url)
            self.q.put_nowait((url, max_redirect))

    async def crawl(self):
        await orm.create_pool(loop=self.loop, **DB_SETTING)
        # 最后一次回复的串
        updated = await Thread.findAll(orderBy='updated_at desc', limit=1)
        self.last_updated = updated[0] if updated else None
        # 最后一次创建的串
        created = await Thread.findAll(orderBy='created_at desc', limit=1)
        self.last_created = created[0] if created else None

        workers = [asyncio.Task(self.work(), loop=self.loop)
                   for _ in range(self.max_tasks)]
        self.t0 = time.time()
        await self.q.join()
        self.t1 = time.time()
        for w in workers:
            w.cancel()
        await orm.close_pool()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    crawler = Crawler()
    loop.run_until_complete(crawler.crawl())
    print('Finished {} urls in {:.3f} secs'.format(
          len(crawler.seen_urls), crawler.t1 - crawler.t0))
    crawler.close()
    loop.close()