#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import logging
import os
import sys
import time
from asyncio import Queue
from datetime import datetime

import aiohttp
import psycopg2
import yarl

import orm
from models import Reply, Thread

THREADS_URL = 'http://h.koukuko.com/api/{}?page={}'
REPLYS_URL = 'http://h.koukuko.com/api/t/{}?page={}'
IMAGE_URL = 'http://static.kukuku.cc/{}'
FORUM = {'综合版1': 4, '欢乐恶搞': 20, '料理': 32, '貓版': 40, '日记': 89, 
         '速报': 83, '推理': 11, '技术宅': 30, '体育': 33, '音乐': 35, 
         '军武': 37, '模型': 39, '考试': 56, '数码': 75, '动画': 14, 
         '漫画': 12, '美漫': 90, '小说': 19, '轻小说': 87, '二次创作': 17, 
         '东方Project': 5, 'VOCALOID': 6, '辣鸡': 95, '游戏': 2, 
         'Minecraft': 10, 'LOL': 22, 'D3': 23, '索尼': 24, '任天堂': 25, 
         '怪物猎人': 28, '日麻': 92, '舰娘': 93, '辐射': 96, 'LoveLive': 97, 
         'MUG': 34, 'WOW': 44, '卡牌桌游': 45, 'MUGEN': 48, 'WOT': 51, 
         '扩散性百万亚瑟王': 63, 'DOTA': 70, 'DNF': 72, 'EVE': 73, 
         '炉石传说': 80, '战争雷霆': 86, 'COSPLAY': 13, 'AKB': 16, '影视': 31, 
         '摄影': 54, '声优': 55, '值班室': 18, '询问2': 36}
IMAGE_FOLDER = os.path.join(sys.path[0], 'static')

# 版面
FORUM_TYPE = ['综合版1', '动画', '询问2', '数码']
# 抓取页
MAX_PAGES = 200
# postgresql设置
DB_SETTING = {
    'user': 'postgres',
    'password': '8523',
    'database': 'kukuku'
}

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

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
        self._stopped = {}.fromkeys(FORUM_TYPE, False)
        self.last_thread = {}.fromkeys(FORUM_TYPE, None)
        for t in FORUM_TYPE:
            self.add_url(yarl.URL(THREADS_URL.format(t, 1)))
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
        except psycopg2.IntegrityError as e:
            await r.update()

    async def parse_thread_link(self, response):
        '''抓取串'''
        links = set()
        next_url = None
        body = await response.json()
        if response.status == 200:
            forum = body['forum']['name']
            for thread in body['data']['threads']:
                # 排除综1置顶串
                thread_id = thread['id']
                if thread_id == 6960723:
                    continue
                # 断点续爬
                updated_at = timestamp2datetime(thread['updatedAt'])
                if (self.last_thread[forum] and 
                        self.last_thread[forum].getValue('updated_at') >= updated_at):
                    self._stopped[forum] = True
                    break
                await self.save_data(thread)
                # 串内回复url
                pages = (thread['replyCount'] + 19) // 20
                urls = [REPLYS_URL.format(thread_id, i)
                        for i in range(1, pages + 1)]
                links.update(urls)

            size = body['page']['size']
            location = body['page']['page']
            if location < (min(size, MAX_PAGES) if isinstance(MAX_PAGES, int) else size):
                next_url = response.url.with_query({'page': location + 1})

        return links, next_url

    async def parse_reply_link(self, response):
        '''抓取回复'''
        body = await response.json()
        if response.status == 200:
            forum = body['forum']['name']
            for reply in body['replys']:
                created_at = timestamp2datetime(reply['createdAt'])
                if (self.last_thread[forum] is None or
                        self.last_thread[forum].getValue('updated_at') < created_at):
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
            if 'h.koukuko.com/api/t/' not in str(url):
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
                asyncio.sleep(1)
        except asyncio.CancelledError:
            pass

    def add_url(self, url, max_redirect=None):
        if max_redirect is None:
            max_redirect = self.max_redirect
        forum = url.path.split('/')[-1]
        if not self._stopped.get(forum):
            self.seen_urls.add(url)
            self.q.put_nowait((url, max_redirect))

    async def crawl(self):
        await orm.create_pool(loop=self.loop, **DB_SETTING)
        for forum in FORUM_TYPE:
            forum_id = FORUM[forum]
            # 最后一次回复的串
            updated = await Thread.findAll('forum=?', [forum_id],
                                            orderBy='updated_at desc', 
                                            limit=1)
            self.last_thread[forum] = updated[0] if updated else None

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

    loop.stop()
    loop.run_forever()
    loop.close()
