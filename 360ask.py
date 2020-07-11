# coding: utf-8
import asyncio
import aiohttp
import aiomysql
import ssl
import certifi
import chardet
import os
import logging
from urllib.parse import quote_plus, urlparse, urljoin
from lxml import etree
import re
from random import randint
import json
import jieba
import time

# windows 系统专用事件循环
try:
    policy = asyncio.WindowsSelectorEventLoopPolicy()
    asyncio.set_event_loop_policy(policy)
except AttributeError:
    pass

curdir = os.path.dirname(os.path.abspath(__file__))
os.chdir(curdir)
logging.basicConfig(level=logging.INFO,
                    format="[%(asctime)s %(levelname)s] %(funcName)s [%(lineno)d] <%(message)s>",
                    datefmt="%Y-%m-%d %H:%M:%S")
jieba.initialize()


class SoAskSpider:
    def __init__(self, config):
        self.seens_url = set()
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.load_verify_locations(certifi.where())
        self.queue = asyncio.PriorityQueue()
        self.config = config
        self._workers = 0
        self._max_workers = config['max_workers']
        self._newline = ["div", "li", "p", "h1", "h2", "h3", "h4", "h5", "tr", "img", "br"]
        self.filter = re.compile(r'展开全部')
        self.search_path = re.compile(r"https://wenda.so.com/search/.*")
        self.question_xpath = re.compile(r"https://wenda.so.com/q/\d+")
        self.record = open('record.txt', 'a+', encoding="utf-8")
        self.relate = open(config["relate_file_name"], 'a+', encoding="utf-8")
        self.header_file = "headers.json"
        self.record_words = set(w.strip() for w in self.record)
        self.headers = {}
        self.folder_path = ''
        self.invalid_charter = re.compile(r'[。？?\\/|…<>"“”：:\xa0\s]')

    async def init(self):
        self.reset_headers()
        if self.config['save_path'] != 'mysql':
            self.make_folder()
        with open(self.config["target_file"], encoding="utf-8") as kws:
            for line in kws:
                wd = line.strip()
                if not wd or wd in self.record_words:
                    continue
                query = f'https://wenda.so.com/search/?q={quote_plus(wd)}&__f=jackpot'
                priority = randint(1, 5)
                await self.queue.put((priority, (query, wd)))

    def make_folder(self):
        folder_path = os.path.join(curdir, self.config['save_path'])
        self.folder_path = folder_path
        if os.path.exists(folder_path):
            return
        os.mkdir(folder_path)

    def reset_headers(self):
        with open(self.header_file, encoding="utf-8") as f:
            try:
                self.headers = json.load(f, encoding="utf-8")
            except json.JSONDecodeError:
                pass

    async def fetch(self, session, url, word, timeout, headers=None, binary=False, proxy=None):
        _headers = self.headers
        if headers:
            _headers = headers
        try:
            async with session.get(url, headers=_headers, timeout=timeout, proxy=proxy, allow_redirects=False) as resp:
                status_code = resp.status
                redirect_url = resp.url
                if status_code == 302:
                    redirect_url = resp.headers.get("Location", resp.url)
                if binary:
                    text = await resp.read()
                    encoding = chardet.detect(text)['encoding']
                    if encoding is None:
                        encoding = "utf-8"
                    html = text.decode(encoding, errors='ignore')
                else:
                    html = await resp.text()
        except (TimeoutError, asyncio.TimeoutError, ConnectionResetError):
            if url in self.seens_url:
                self.seens_url.remove(url)
            priority = randint(1, 5)
            await self.queue.put((priority, (url, word)))
            status_code = 0
            redirect_url = url
            html = None
        except (aiohttp.ClientError, asyncio.CancelledError, RuntimeError) as err:
            logging.error(f'fetching {url} error {err}')
            status_code = 0
            redirect_url = url
            html = None
        return status_code, html, redirect_url

    @staticmethod
    def get_host(url):
        net_loc = urlparse(url).netloc
        return net_loc

    async def savedb(self, pool, title, content, url, word):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    insert_sql = f"INSERT INTO `{self.config['table_name']}` (`title`, `content`, `url`, `word`) VALUES (%s,%s,%s,%s)"
                    await cur.execute(insert_sql, args=(title, content, url, word))
                except aiomysql.MySQLError as err:
                    logging.error(f"insert data error: {err}")

    def save_local(self, title, content, word):
        title = self.invalid_charter.sub('', title)
        filepath = os.path.join(self.folder_path, f"{word}[#]{title}.txt")
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)
        except OSError as err:
            logging.error(f"{title} write file error {err}")

    async def process(self, session, pool, url, word, timeout):
        status, source, redirect_url = await self.fetch(session, url, word, timeout)
        if status == 200:
            if self.search_path.match(url):
                self.parse_list(source, url, word)
            if self.question_xpath.match(url):
                title, content = self.parse_detail(source)
                if len(content) > 50:
                    if self.config['save_path'] == 'mysql':
                        await self.savedb(pool, title, content, url, word)
                    else:
                        self.save_local(title, content, word)
                    if word not in self.record_words:
                        self.record.write(f'{word}\n')
                        self.record.flush()
                        self.record_words.add(word)
        elif status == 302:
            if re.match(r'/other/jackpot.*?', redirect_url):
                if url in self.seens_url:
                    self.seens_url.remove(url)
                priority = randint(1, 5)
                self.queue.put_nowait((priority, (url, word)))
                logging.error(f"出现验证码，请打开网址重新获取cookie和refer: {urljoin(url, redirect_url)}")
                await asyncio.sleep(60)
                self.reset_headers()
            else:
                logging.warning(f"{url} response 302 ststus, redirect: {redirect_url}")
        self._workers -= 1

    def parse_list(self, source, url, word):
        try:
            doc = etree.HTML(source)
        except Exception as err:
            logging.error(f"parse error: {err}")
        else:
            atags = doc.xpath('//div[@class="qa-i-hd"]/h3/a')
            for item in atags:
                link = item.attrib.get('href')
                if link is None:
                    continue
                link = urljoin(url, link)
                priority = randint(1, 5)
                self.queue.put_nowait((priority, (link, word)))
            relate_words = doc.cssselect('div#js-search-rel th a')
            for item in relate_words:
                w = item.text.strip()
                if w.endswith('...'):
                    continue
                self.relate.write(f'{w}\n')
                self.relate.flush()
            del doc

    def parse_detail(self, source):
        try:
            doc = etree.HTML(source)
        except Exception as err:
            logging.error(f"parse detail error: {err}")
            title = ""
            content_text = ""
        else:
            title = ''.join(doc.xpath('//h2[contains(@class, "title")]/text()'))
            content_item = doc.xpath('//div[contains(@class, "resolved-cnt")]/node()')
            if content_item:
                content_text = self.get_text(content_item)
            else:
                content_text = ""
        return title, content_text

    def get_text(self, nodes):
        texts = ""
        for node in nodes:
            if etree.iselement(node):
                if node.tag == 'div':
                    texts += '\n'
                    continue
                if node.tag in self._newline:
                    texts += '\n'
                nodes = node.xpath('./node()')
                texts += self.get_text(nodes)
            else:
                if self.filter.search(node):
                    continue
                text = node.strip()
                if text:
                    texts += f"{text}"
        return texts.strip()

    async def loop(self):
        await self.init()
        timeout = aiohttp.ClientTimeout(total=5)
        conn = aiohttp.TCPConnector(ssl=self.ssl_context,
                                    limit=1000,
                                    enable_cleanup_closed=True)
        session = aiohttp.ClientSession(connector=conn, timeout=timeout)
        pool = await aiomysql.create_pool(**self.config['dbconfig'])
        last_rate_time = time.time()
        while 1:
            if self._workers > self._max_workers:
                await asyncio.sleep(3)
                continue
            if self.queue.empty():
                await asyncio.sleep(1)
                if self._workers == 0:
                    break
                continue
            _, item = await self.queue.get()
            query, word = item
            if query in self.seens_url:
                continue
            self.seens_url.add(query)
            asyncio.ensure_future(self.process(session, pool, query, word, timeout))
            self._workers += 1
            gap = time.time() - last_rate_time
            if gap > 10:
                logging.info(f"Qsize: {self.queue.qsize()} workers: {self._workers})")
                last_rate_time = time.time()
        await session.close()

    def run(self):
        try:
            asyncio.run(self.loop())
        except KeyboardInterrupt:
            logging.info('Bye!')


if __name__ == "__main__":
    dbconfig = dict(
        host='127.0.0.1',
        port=3306,
        user='root',  # msyql用户名
        password='root',  # MySQL密码
        db='360spider',  # 数据库名称
        autocommit=True,
        charset='utf8mb4'  # 数据库的字符集编码
    )
    crawl_config = dict(
        target_file="juzi.txt",  # 要抓取的关键词名称
        dbconfig=dbconfig,
        table_name="juzi",  # 数据保存到哪个表里面
        relate_file_name="juzi-relate.txt",  # 相关关键词保存文件名称
        save_path="mysql",  # 如果是mysql，那么数据就会保存到MySQL数据库，如果是其它名称，那么就会保存到指定的文件夹
        max_workers=150,  # 线程数量
    )
    so = SoAskSpider(crawl_config)
    so.run()
