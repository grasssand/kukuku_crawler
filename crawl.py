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

# import orm
from log import log_info
# from models import Thread, Reply

THREADS_URL = 'http://h.koukuko.com/api/{}?page={}'
REPLYS_URL = 'http://h.koukuko.com/api/t/{}?page={}'
IMAGE_URL = 'http://static.kukuku.cc/{}'
FORUM_TYPE = ['综合版1', '询问2']
IMAGE_FOLDER = 'static'
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

    async def save_db(self, item):
        if item['image']:
            print('download image: ', item['image'])
            await self.image_download(item['image'])
        item['created_at'] = datetime.fromtimestamp(item['createdAt'] / 1000)
        # if item.get('parent'):
        #     r = Reply(**item)
        # else:
        #     r = Thread(**item)
        # try:
        #     await r.save()
        # except Exception as e:
        #     LOGGER.error('save database error: %r', e)

    async def parse_link(self, response):
        links = set()
        next_url = None
        body = await response.json()

        if response.status == 200:
            max_pages = body['page']['size']
            location_page = body['page']['page']
            if body.get('replys') is not None:
                for reply in body['replys']:
                    await self.save_db(reply)
                    # pass
            else:
                for thread in body['data']['threads']:
                    link = REPLYS_URL.format(thread['id'], 1)
                    links.add(link)
                    await self.save_db(thread)
            if location_page < max_pages:
                url = response.url.with_query(None)
                next_url = '{}?page={}'.format(url, location_page + 1)
        return next_url, links

    async def fetch(self, url, max_redirect):
        print('fetch url: %s' % url)
        tries = 0
        exception = None
        while tries < self.max_tries:
            try:
                response = await self.session.get(url, allow_redirects=False)
                break
            except aiohttp.ClientError as client_error:
                LOGGER.info('try %r for %r raised %r', tries, url, client_error)
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
                next_url, links = await self.parse_link(response)
                if next_url in self.seen_urls:
                    return
                if next_url is not None:
                    self.add_url(next_url)
                for link in links.difference(self.seen_urls):
                    self.q.put_nowait((link, self.max_redirect))
                self.seen_urls.update(links)
        finally:
            await response.release()

    async def work(self):
        try:
            while True:
                url, max_redirect = await self.q.get()
                assert url in self.seen_urls
                await self.fetch(url, max_redirect)
                self.q.task_done()
                asyncio.sleep(1)
        except asyncio.CancelledError:
            pass

    def add_url(self, url, max_redirect=None):
        if max_redirect is None:
            max_redirect = self.max_redirect
        self.seen_urls.add(url)
        self.q.put_nowait((url, max_redirect))

    async def crawl(self):
        # await orm.create_pool(loop=self.loop, **DB_SETTING)
        workers = [asyncio.Task(self.work(), loop=self.loop)
                   for _ in range(self.max_tasks)]
        self.t0 = time.time()
        await self.q.join()
        self.t1 = time.time()
        for w in workers:
            w.cancel()
        # await orm.destroy_pool()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    crawler = Crawler()
    loop.run_until_complete(crawler.crawl())
    print('Finished {} urls in {:.3f} secs'.format(
          len(crawler.seen_urls), crawler.t1 - crawler.t0))
    crawler.close()

    loop.close()