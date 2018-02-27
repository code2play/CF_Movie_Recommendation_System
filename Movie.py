class movie:
    def __init__(self):
        self.id = 0
        self.title = ''
        self.year = 0
        self.director = []
        self.writer = []
        self.starring = []
        self.genre = []
        self.country = []
        self.language = []
        self.length = 0
        self.rating = 0.0

    # 输出电影信息，用于测试
    def print_info(self):
        print('id: {}'.format(self.id))
        print('title: ' + self.title)
        print('year: {}'.format(self.year))
        print('director: ', self.director)
        print('writer: ', self.writer)
        print('starring: ', self.starring)
        print('genre: ', self.genre)
        print('country: ', self.country)
        print('language: ', self.language)
        print('length: {}'.format(self.length))
        print('ratting: {}'.format(self.rating))

    # 生成sql语句
    def generate_sql(self):
        SQL = []

        genre = self.genre[0].strip()
        if len(self.genre)>1:
            for i in range(1,len(self.genre)):
                genre += '/'
                genre += self.genre[i].strip()

        countrys = self.country[0].split('/')
        country = countrys[0].strip()
        if len(countrys)>1:
            for i in range(1,len(countrys)):
                country += '/'
                country += countrys[i].strip()

        languages = self.language[0].split('/')
        language = languages[0].strip()
        if len(languages)>1:
            for i in range(1,len(languages)):
                language += '/'
                language += languages[i].strip()

        sql = "INSERT INTO movie VALUES(" + \
              str(self.id) + ',' + \
              "'" + self.title + "'," + \
              str(self.year) + ',' + \
              "'" + genre + "'," + \
              "'" + country + "'," + \
              "'" + language + "'," + \
              str(self.length) + ',' + \
              str(self.rating) + ')'
        SQL.append(sql)


        for item in self.director:
            sql = "INSERT INTO director VALUES(" + \
                  str(self.id) + ',' + \
                  "'" + item + "')"
            SQL.append(sql)

        for item in self.writer:
            sql = "INSERT INTO writer VALUES(" + \
                  str(self.id) + ',' + \
                  "'" + item + "')"
            SQL.append(sql)

        for item in self.starring:
            sql = "INSERT INTO starring VALUES(" + \
                  str(self.id) + ',' + \
                  "'" + item + "')"
            SQL.append(sql)

        return SQL