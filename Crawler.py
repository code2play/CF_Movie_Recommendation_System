from selenium import webdriver
import time
import pymysql
from multiprocessing import Process, Queue, cpu_count, Lock
import requests
import re
from Movie import movie

db = pymysql.connect("localhost", "root", "123456", "douban")
cursor = db.cursor()

headers = {
    'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'
}

# cookie模拟登陆，可能导致账号被锁定
def getCookies(fileName):
    cookies = {}
    with open(fileName,'r') as f:
        for line in f.read().split(';'):
            name, value = line.strip().split('=', 1)
            cookies[name] = value
    return cookies
# cookies = getCookies('Cookie.txt')

# 爬取电影url
def crawl_url():
    browser = webdriver.Chrome()
    browser.get("https://movie.douban.com/tag/#/?sort=T&range=0,10&tags=%E7%94%B5%E5%BD%B1")

    time.sleep(5)
    # 页面动态加载，点击加载更多
    more = browser.find_element_by_class_name('more')

    for i in range(50):
        more.click()
        time.sleep(5)

    movies = browser.find_elements_by_class_name('item')
    cnt = 0
    for item in movies:
        url = item.get_attribute('href')
        if url.startswith('https://'):
            sql = "INSERT INTO urls VALUES('" + url + "', 0)"
            try:
                cursor.execute(sql)
                db.commit()
                cnt += 1
                if cnt % 50 == 0:
                    print('{} urls saved'.format(cnt))
            except:
                db.rollback()
                print('    Error: ' + sql)
    browser.close()

# 删除所有数据，测试用
def delete_all_saved_data():
    SQL = ['DELETE FROM director',
           'DELETE FROM movie',
           'DELETE FROM review',
           'DELETE FROM starring',
           'DELETE FROM writer']
    flag = True
    for sql in SQL:
        try:
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()
            flag = False
            print('    Error: ' + sql)
    if flag:
        print('Successfully Deleted All Saved Data')


class crawler(Process):
    def __init__(self, id, urls, lock):
        Process.__init__(self)
        self.id = id
        self.urls = urls
        self.lock = lock
        self.current_mid = 0    # current movie id

    def run(self):
        print('Process-{} Started'.format(self.id))

        while not self.urls.empty():
            url = self.urls.get()
            print('Process-{} get {}'.format(self.id, url))
            self.crawl_data(url)
            time.sleep(10)
            self.crawl_review(url)
            # 爬取完电影数据和评论数据之后将url的used标记为1
            sql = 'UPDATE urls SET used=1 WHERE url=' + "'" + url + "'"
            self.execute_sql(sql)

            time.sleep(10)

    def execute_sql(self, sql):
        try:
            cursor.execute(sql)
            db.commit()
            return True
        except:
            db.rollback()
            # print('    Error: ' + sql)
            return False

    # 爬取电影数据
    def crawl_data(self, url):
        r = requests.get(url, headers=headers)
        m = movie()
        self.current_mid = m.id = url.split('/')[-2]
        # 电影名
        pattern = re.compile('<span property="v:itemreviewed">(.*)</span>')
        m.title = re.findall(pattern, r.text)
        while m.title == []:    # 电影名为空说明没有获取到页面
            print('Can NOT Reach {}, Sending Request Again'.format(url))
            time.sleep(10)
            r = requests.get(url, headers=headers)
            m.title = re.findall(pattern, r.text)
        m.title = m.title[0]
        # 年份
        pattern = re.compile('<span class="year">\((\d*)\)</span>')
        m.year = re.findall(pattern, r.text)[0]
        # 导演
        pattern = re.compile('rel="v:directedBy">([^<]*)')
        m.director = re.findall(pattern, r.text)
        # 编剧
        pattern = re.compile('<a href="/celebrity/\d*/">([^<]*)</a>')
        m.writer = re.findall(pattern, r.text)
        # 主演
        pattern = re.compile('rel="v:starring">([^<]*)</a>')
        m.starring = re.findall(pattern, r.text)
        # 类型
        pattern = re.compile('<span property="v:genre">([^<]*)</span>')
        m.genre = re.findall(pattern, r.text)
        # 国家
        pattern = re.compile('制片国家/地区:</span>([^<]*)')
        m.country = re.findall(pattern, r.text)
        # 语言
        pattern = re.compile('语言:</span>([^<]*)')
        m.language = re.findall(pattern, r.text)
        # 长度
        if m.id == '3734350':   # 两个特殊的页面，结构特殊，下面正则表达式获取不到片长
            m.length = 6
        elif m.id == '6146955':
            m.length = 81
        else:
            pattern = re.compile('property="v:runtime" content="(\d+)')
            m.length = re.findall(pattern, r.text)[0]
        #评分
        pattern = re.compile('property="v:average">([\d\.]+)</strong>')
        m.rating = re.findall(pattern, r.text)[0]

        # m.print_info()

        SQL = m.generate_sql()
        flag = True
        for sql in SQL:
            if self.execute_sql(sql) == False:
                flag = False
        if flag:
            print('Process-{} Successfully Saved {}'.format(self.id, m.title))

    # 爬取评分
    def crawl_review(self, url):
        root_url = url + 'reviews'
        r = requests.get(root_url, headers=headers)
        # 评论总页数
        pattern = re.compile('<span class="thispage" data-total-page="(\d*)">')
        total_page = re.findall(pattern, r.text)
        if total_page == []:
            total_page = 1
        else:
            total_page = int(total_page[0])
        # 每个电影最多爬取10页(200)条评分
        for page in range(min(total_page, 10)):
            if page>0:
                url = root_url + '?start=' + str(page*20)
                r = requests.get(url, headers=headers)

            pattern = re.compile('<a href="https://www.douban.com/people/(.*)/" property="v:reviewer" '
                                 'class="name">.*</a>[.\s]*<span property="v:rating" '
                                 'class="allstar(\d*) main-title-rating"')
            res = re.findall(pattern, r.text)
            user_id = [res[i][0] for i in range(len(res))]
            rating = [res[i][1] for i in range(len(res))]

            flag = True
            for i in range(len(user_id)):
                uid = user_id[i]
                rat = rating[i]
                sql = "INSERT INTO review VALUES(" + \
                      str(self.current_mid) + "," + \
                      "'" + str(uid) + "'," + \
                      rat + ')'
                if self.execute_sql(sql)==False:
                    flag = False

            if flag:
                print('Process-{} Successfully Saved Reviews of {} in Page {}'.
                      format(self.id, self.current_mid, page))
            else:   # 未能完全存储打分信息，一个人可能写多条影评，由于数据库主键约束，无法存储
                print('Process-{} Finished Processing Reviews of {} in Page {}'.
                      format(self.id, self.current_mid, page))

            time.sleep(10)

if __name__ == '__main__':
    # crawl_url()   # 爬取url，只需运行一次
    n = cpu_count()
    lock = Lock()

    urls = Queue()
    cursor.execute('SELECT url FROM urls WHERE used=0')
    allurls = cursor.fetchall()
    for url in allurls:
        urls.put(url[0])

    pool = []
    for i in range(n):
        p = crawler(i, urls, lock)
        p.start()
        pool.append(p)
        time.sleep(3)
    for p in pool:
        p.join()

    print('THE END')
