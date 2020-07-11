# coding: utf-8
import random
import re
import requests
import time
import pymysql
from threading import Thread
from queue import Queue


class PostArticle(Thread):
    def __init__(self, config):
        super().__init__()
        self.config = config

    def run(self):
        while 1:
            try:
                word = self.config['queue'].get()
                articles = self.get_article(word)
                if not articles:
                    continue
                article = self.make_article(word, articles)
                data = self.generate_form(word, article)
                res = self.post(data)
                if res is None:
                    continue
                print(f'post [{word}] {res}')
            finally:
                self.config['queue'].task_done()

    def get_article(self, word):
        conn = pymysql.connect(**self.config['dbconf'])
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT title, content FROM `{self.config['table_name']}` WHERE `word`=%s",
                           args=word)
            results = cursor.fetchall()
        conn.close()
        return results

    def make_article(self, word, articles):
        new_title = word
        new_content = ""
        nums = random.choice(self.config['cnums'])
        if len(articles) < nums:
            nums = len(articles)
        choice_articles = random.sample(articles, nums)
        for article in choice_articles:
            title = article['title']
            new_content += f"<h3>{title}</h3>"
            content = article['content']
            for line in re.split(r'\n+', content):
                new_content += f"<p>{line}</p>"
        return {"title": new_title, "contents": new_content}

    def post(self, form_data):
        _headers = {"User-Agent": "Article-poster/1.0"}

        try:
            resp = requests.post(self.config['backend'],
                                 data=form_data,
                                 headers=_headers,
                                 timeout=60)
        except requests.Timeout:
            return self.post(form_data)
        except requests.RequestException as err:
            print(f"post article[{form_data['title']}] error: <{err}>")
            result = None
        else:
            result = resp.text
        return result

    def generate_form(self, keywords, article):
        cms = self.config['cms']
        if cms == "phpcms":
            return {
                'cid': self.config['catid'],
                'title': article['title'],
                'description': re.sub(r'(</?[^>]+>|\s)', '', article['contents'])[:80],
                'content': article['contents'],
                'copyfrom': '互联网',
                'keywords': keywords,
                'date': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            }

        if cms == "dedecms":
            return {
                'username': 'admin',  # 这里要改成自己的用户名
                'channelid': 1,
                'dopost': 'save',
                'title': article['title'],
                'tags': keywords,
                'picname': '',
                'source': "互联网",  # 来源
                'writer': "admin",
                'typeid': self.config['catid'],  # 分类id
                'keywords': keywords,  # 关键词
                'autokey': 1,
                'description': re.sub(r'(</?[^>]+>|\s)', '', article['contents'])[:80],
                'remote': 1,
                'autolitpic': 1,
                'sptype': 'hand',
                'spsize': 5,
                'body': article['contents'],
                'click': random.randint(100, 999),
                'arcrank': 0,  # 阅读权限
                'pubdate': time.strftime('%Y-%m-%d %H:%M:%S',
                                         time.localtime()),
                'ishtml': 0,
                'imageField.x': 40,
                'imageField.y': 11
            }

        if cms == "empirecms":
            return {
                'username': 'admin',  # 填写自己的用户名
                'enews': 'AddNews',  # 添加新闻
                'classid': self.config['catid'],  # 分类id
                'bclassid': 0,  # 父分类id
                'id': 0,  # 文章id
                'ecmsnfrom': 1,
                'title': article['title'],
                'checked': 1,
                'keyboard': keywords,  # tag标签
                'newstime': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                'smalltext': re.sub(r'(</?[^>]+>|\s)', '', article['contents'])[:80],
                'writer': 'admin',
                'befrom': '互联网',  # 来源
                'newstext': article['contents'],  # 内容
                'dokey': 1,  # 关键字替换
                'copyimg': 1,  # 远程保存图片
                'autosize': 5000,
                'getfirsttitlepic': 1,
                'getfirsttitlespicw': 105,
                'getfirsttitlespich': 118,
                'istop': 0,
                'newspath': time.strftime('%Y-%m-%d', time.localtime()),  # 文件路径
                'addnews': '提交'
            }


def load_post_words(queue, file_name):
    with open(file_name, encoding='utf-8') as f:
        for line in f:
            word = line.strip()
            queue.put(word)


if __name__ == "__main__":
    # 数据库连接配置
    dbconfig = dict(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="root",  # 改成自己设置的密码
        db="360spider",  # 改成自己的数据库名称
        charset="utf8",
        cursorclass=pymysql.cursors.DictCursor)

    # 要发布内容的关键词文件
    keyword_file = "juzi.txt"
    word_queue = Queue()
    load_post_words(word_queue, keyword_file)

    post_config = dict(
        dbconf=dbconfig,
        table_name="juzi",  # 关键词对应的数据库内容表
        catid=1,  # 要发布的分类id
        cms="empirecms",  # 要发布的程序名, phpcms, dedecms, empirecms
        backend="http://www.diguocms.com/e/admin/jiekou.php?pw=123456",  # 发布的后台地址
        queue=word_queue,
        cnums=[3, 6]  # 关键词聚合内容的数量
    )

    for i in range(10):
        pa = PostArticle(post_config)
        pa.setDaemon(True)
        pa.start()

    word_queue.join()
    print("Done")
