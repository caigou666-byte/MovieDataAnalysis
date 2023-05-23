import string
import random
from flask import Flask
from concurrent.futures import ThreadPoolExecutor
import DataBase
from flask import Flask, render_template
from flask import *
import json
import scrapy
import copy
import threading
from multiprocessing import Process, Manager
from scrapy.crawler import CrawlerProcess
class Data:
    DataBase = None
    data = {}  # 弹幕与评论数据,请求ID为键，数据为值

# 存储任务状态的字典
task_statuses = {}
# 创建线程池执行器
executor = ThreadPoolExecutor(10)
app = Flask(__name__)

##########################################################################
######################     Flask接口           ###########################
##########################################################################

@app.route('/', methods=["GET"])
def homePage():
    return render_template('index.html')


@app.route('/submitTask_getMovieDataFromDatabase', methods=["POST"])
def submitTask_getMovieDataFromDatabase():
    """
    提交任务_得到数据来自数据库的接口
    """
    randomID = generate_random_string()
    setTaskStatusToInProgress(randomID)
    executor.submit(longTask_getMovieDataFromDatabase, randomID)  # 创建异步长任务
    return {"randomID": randomID}


def longTask_getMovieDataFromDatabase(randomID):
    # SQL = "select * from movieData;"
    # data = Data.DataBase.Query(SQL)
    setTaskStatusToSuccess(randomID)
    # print(result)
    result = {"data1": readFromSqliteOfMaoyan(
    ), "data2": readFromSqliteOfDouban()}
    task_statuses[randomID]["result"] = result


class maoyan_MovieSpider(scrapy.Spider):
    name = 'maoyan_MovieSpider'
    MovieNameParam = []
    start_urls = []
    custom_settings = {
        'CONCURRENT_REQUESTS': 5,
        'DOWNLOAD_DELAY': 1,
    }
    queue = None

    def start_requests(self):
        # 添加Cookies信息
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0"}
        # 指定cookies
        cookies = {
            'uuid': '66a0f5e7546b4e068497.1542881406.1.0.0',
                    '_lxsdk_cuid': '1673ae5bfd3c8-0ab24c91d32ccc8-143d7240-144000-1673ae5bfd4c8',
                    '__mta': '222746148.1542881402495.1542881402495.1542881402495.1',
                    'ci': '20',
                    'rvct': '20%2C92%2C282%2C281%2C1',
                    '_lx_utm': 'utm_source%3DBaidu%26utm_medium%3Dorganic',
                    '_lxsdk_s': '1674f401e2a-d02-c7d-438%7C%7C35'}
        # 发送请求并添加Cookies信息
        for url in self.start_urls:
            yield scrapy.Request(url, cookies=cookies, headers=headers, callback=self.parse)

    def parse(self, response):
        # 使用CSS选择器查找所有class为movie-card的元素
        movie_cards = response.css('.movie-card')
        # 打印所有movie-card元素的文本内容
        for movie_card in movie_cards:
            # 使用CSS选择器查找class为box red的div元素，并判断是否存在
            box_red = movie_card.css('div.box.red')
            if box_red:
                # print(
                #     "=======================================================================")
                # 使用CSS选择器查找class为.type.ellipsis-1的div元素，并获取其中的所有文本内容
                movieName = ''.join(movie_card.css(
                    '.name *::text').getall()).strip()
                # print()
                if movieName in self.MovieNameParam:
                    data = {"movieName": movieName,
                            "boxOffice": ''.join(text.strip() for text in box_red.css('*::text').getall()),
                            "type": ''.join(movie_card.css('.type.ellipsis-1 *::text').getall()).strip(),
                            "cast": ''.join(movie_card.css('.cast.ellipsis-1 *::text').getall()).strip(),
                            "time": ''.join(movie_card.css('.time.ellipsis-1 *::text').getall()).strip(),
                            }
                    print(
                        "=======================================================================")
                    print(data)
                    self.queue.put(json.dumps(data).encode())


class douban_MovieSpider(scrapy.Spider):
    name = 'douban_MovieSpider'
    MovieNameParam = []
    start_urls = []
    custom_settings = {
        'CONCURRENT_REQUESTS': 5,
        'DOWNLOAD_DELAY': 1,
    }
    queue = None
    # 添加Cookies信息
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.54"}
    # 指定cookies
    cookies = {
        'uuid': '66a0f5e7546b4e068497.1542881406.1.0.0',
        '_lxsdk_cuid': '1673ae5bfd3c8-0ab24c91d32ccc8-143d7240-144000-1673ae5bfd4c8',
        '__mta': '222746148.1542881402495.1542881402495.1542881402495.1',
        'ci': '20',
        'rvct': '20%2C92%2C282%2C281%2C1',
        '_lx_utm': 'utm_source%3DBaidu%26utm_medium%3Dorganic',
        '_lxsdk_s': '1674f401e2a-d02-c7d-438%7C%7C35'
    }

    def start_requests(self):
        # 发送请求并添加Cookies信息
        # print(self.start_urls)
        for url in self.start_urls:
            yield scrapy.Request(url, cookies=self.cookies, headers=self.headers, callback=self.parse)

    def parse(self, response):
        # 使用CSS选择器查找所有class为movie-card的元素
        json_data = json.loads(response.text)[0]
        yield scrapy.Request(json_data["url"], cookies=self.cookies, headers=self.headers, callback=self.parse_other, meta=copy.deepcopy(json_data))

    def parse_other(self, response):
        # 处理其他页面的内容
        info = ''.join(response.css('div#info *::text').getall()
                       ).strip().split("\n")
        rank = ''.join(response.css(
            'strong.ll.rating_num *::text').getall()).strip()
        PlotSummary = ''.join(response.css(
            'div#link-report-intra *::text').getall()).strip().replace(" ", "").replace("\n", "")
        comment = []
        for i in response.css('p.comment-content'):
            comment.append(''.join(i.css("*::text").getall()).strip())
            # print()
        info = [s.strip() for s in info]
        info = [x for x in info if x != '']
        info = {s.split(':')[0]: s.split(':')[1].strip() for s in info}
        result = {"rank": rank,
                  "PlotSummary": PlotSummary,
                  "comment": comment,
                  "info": info,
                  "picSrc": response.meta
                  }
        self.queue.put(json.dumps(result).encode())


def maoyan_crawl(queue, MovieNameParam, start_urls, is_obey):
    process = CrawlerProcess(settings={
        'ROBOTSTXT_OBEY': is_obey
    })
    process.crawl(maoyan_MovieSpider, queue=queue,
                  MovieNameParam=MovieNameParam, start_urls=start_urls)
    process.start()


def douban_crawl(queue, MovieNameParam, start_urls, is_obey):
    process = CrawlerProcess(settings={
        'ROBOTSTXT_OBEY': is_obey
    })
    process.crawl(douban_MovieSpider, queue=queue,
                  MovieNameParam=MovieNameParam, start_urls=start_urls)
    process.start()


class maoyan_crawlerThread(threading.Thread):
    def __init__(self, queue, randomID):
        super(maoyan_crawlerThread, self).__init__()
        self.queue = queue
        self.randomID = randomID

    def run(self):
        print("!!!!", task_statuses[self.randomID])
        task_statuses[self.randomID]["result"] = []
        print("!!!!", task_statuses[self.randomID])
        count = len(getMovieName())
        while True:
            if not self.queue.empty():
                data = json.loads(self.queue.get().decode())
                task_statuses[self.randomID]["result"].append(data)
                # writeIntoSqliteFromMaoyan(self.randomID, data)
                if len(task_statuses[self.randomID]["result"]) >= count:
                    setTaskStatusToSuccess(self.randomID)
                    break


class douban_crawlerThread(threading.Thread):
    def __init__(self, queue, randomID):
        super(douban_crawlerThread, self).__init__()
        self.queue = queue
        self.randomID = randomID

    def run(self):
        print("!!!!",task_statuses[self.randomID])
        task_statuses[self.randomID]["result"] = []
        count = len(getMovieName())
        while True:
            if not self.queue.empty():
                data = json.loads(self.queue.get().decode())
                # print("@@@@@@@@@@@@")
                # print(data)
                task_statuses[self.randomID]["result"].append(data)
                # writeIntoSqliteFromDouban(self.randomID, data)
                if len(task_statuses[self.randomID]["result"]) >= count:
                    setTaskStatusToSuccess(self.randomID)
                    break


@app.route('/submitTask_startCrawlerToGetMovieDataFromMaoyan', methods=["POST"])
def submitTask_startCrawlerToGetMovieDataFromMaoyan():
    """
    提交任务_启动爬虫得到电影数据 来自猫眼 的接口
    """
    randomID = generate_random_string()
    setTaskStatusToInProgress(randomID)
    executor.submit(
        longTask_startCrawlerToGetMovieDataFromMaoyan, randomID)  # 创建异步长任务
    return {"randomID": randomID}


def longTask_startCrawlerToGetMovieDataFromMaoyan(randomID):
    # SQL = "select * from springFestivalMovieNameList;"
    # result = Data.DataBase.Query(SQL)

    MovieNameParam = getMovieName()
    start_urls = ['http://pf.fe.st.maoyan.com/mdb/search?key=' +
                  name for name in MovieNameParam]
    """
启动爬虫线程
    """
    queue = Manager().Queue()  # 创建消息队列
    maoyan_crawlerThread_ = maoyan_crawlerThread(queue, randomID)
    process = Process(target=maoyan_crawl, args=(
        queue, MovieNameParam, start_urls, False))  # 执行爬虫线程
    process.start()
    maoyan_crawlerThread_.start()
    setTaskStatusToInProgress(randomID)


@app.route('/submitTask_startCrawlerToGetMovieDataFromDouban', methods=["POST"])
def submitTask_startCrawlerToGetMovieDataFromDouban():
    """
    提交任务_启动爬虫得到电影数据 来自豆瓣 的接口
    """
    randomID = generate_random_string()
    setTaskStatusToInProgress(randomID)
    executor.submit(
        longTask_startCrawlerToGetMovieDataFromDouban, randomID)  # 创建异步长任务
    return {"randomID": randomID}




def writeIntoSqliteFromMaoyan(randomID, data):
    data = json.dumps(data)
    SQL = "INSERT INTO manyanMovieData (randomID,maoyanData) VALUES ('%s', '%s');" % (str(randomID), data)
    Data.DataBase.Execute(SQL)

# item_id="123"
# SQL="select * from products where category_id=item_id"#错误写法
# SQL="select * from products where category_id="+item_id#正确写法
# SQL="select * from products where category_id='%s'"%(item_id)#正确写法


def writeIntoSqliteFromDouban(randomID, data):
    data = json.dumps(data)
    SQL = "INSERT INTO doubanMovieData (randomID,doubanData) VALUES ('%s', '%s');" % (
        str(randomID), data)
    Data.DataBase.Execute(SQL)


def readFromSqliteOfMaoyan():
    SQL = "select * from manyanMovieData;"
    result = Data.DataBase.Query(SQL)
    
    data=[]
    for i in result:
        data.append(i[1])
    # print(data)
    return data


def readFromSqliteOfDouban():
    SQL = "select * from doubanMovieData;"
    result = Data.DataBase.Query(SQL)
    data=[]
    for i in result:
        data.append(i[1])
    # print(data)    
    return data


def longTask_startCrawlerToGetMovieDataFromDouban(randomID):
    # SQL = "select * from springFestivalMovieNameList;"
    # result = Data.DataBase.Query(SQL)
    # result =getMovieName()
    MovieNameParam =getMovieName()
    start_urls = ['https://movie.douban.com/j/subject_suggest?q=' +
                  name for name in MovieNameParam]
    """
启动爬虫线程
    """
    queue = Manager().Queue()  # 创建消息队列
    douban_crawlerThread_ = douban_crawlerThread(queue, randomID)
    process = Process(target=douban_crawl, args=(
        queue, MovieNameParam, start_urls, False))  # 执行爬虫线程
    process.start()
    douban_crawlerThread_.start()
    setTaskStatusToInProgress(randomID)


@app.route("/checkTaskStatus/<randomID>", methods=["GET"])
def checkTaskStatus(randomID):
    """
    查询任务状态的接口
    """
    pattern = r'^(?=.*\d)(?=.*[A-Z]).{10}$'
    import re
    if re.match(pattern, randomID) == None:
        return {"error": "参数错误"}
    if randomID not in task_statuses:
        # 如果任务 ID 不存在，返回 404 错误
        return {"error": "Task not found."}
    # 返回任务状态
    if task_statuses[randomID]["status"] == "Failed":
        return {"error": "Task Failed"}
    if task_statuses[randomID]["status"] == "Success":
        return task_statuses[randomID]
    if task_statuses[randomID]["status"] == "InProgress":
        return {"error": "Task InProgress"}


def setTaskStatusToFailed(randomID):
    """
    “设置任务状态为失败”
    """
    task_statuses[randomID]["status"] = "Failed"


def setTaskStatusToSuccess(randomID):
    """
    “设置任务状态为成功”
    """
    task_statuses[randomID]["status"] = "Success"


def setTaskStatusToInProgress(randomID):
    """
    “设置任务状态为进行中”
    """
    task_statuses[randomID] = {"status": "InProgress"}


def generate_random_string():
    # 生成包含数字和大写字母的字符集
    char_set = string.digits + string.ascii_uppercase

    # 随机生成10个字符
    random_chars = random.choices(char_set, k=10)

    # 确保生成的字符串包含数字和大写字母
    while not (any(char.isdigit() for char in random_chars) and
               any(char.isupper() for char in random_chars)):
        random_chars = random.choices(char_set, k=10)

    # 将字符列表转换为字符串
    random_string = ''.join(random_chars)
    return random_string
def getScriptPath():
    import os
    # 获取当前Python脚本的绝对路径
    script_path = os.path.abspath(__file__)
    # 获取当前Python脚本所在的目录
    return os.path.dirname(script_path)
def getMovieName():
    file_path = "MovieName.txt"
    # 打开文件并读取每行内容
    with open(getScriptPath()+"\\"+file_path, "r",encoding='utf-8') as file:
        lines = file.readlines()
    return [item.strip("\n") for item in lines]   
if __name__ == '__main__':
    Data.DataBase = DataBase.DataBaseSqlite()
    Data.DataBase.Connect_DataBase()
    import logging
    # logging.basicConfig(filename='app.log', level=logging.INFO)
    # print(getMovieName())
    app.run(host='0.0.0.0', port=80, debug=True)