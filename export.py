import pymysql

db = pymysql.connect("localhost", "root", "tianjiahao1997", "douban")
cursor = db.cursor()

sql = [
    'SELECT * FROM movie',
    'SELECT * FROM director',
    'SELECT * FROM writer',
    'SELECT * FROM starring',
    'SELECT * FROM review',
    'SELECT user_id, COUNT(*) '
        'FROM review '
        'GROUP BY user_id '
        'ORDER BY count(*) DESC '
        'LIMIT 1000'
]

filename = [
    'movie.txt',
    'director.txt',
    'writer.txt',
    'starring.txt',
    'review.txt',
    'top reviewers.txt'
]

for i in range(len(sql)):
    cursor.execute(sql[i])
    res = cursor.fetchall()
    with open(filename[i], 'w', encoding='UTF-8') as f:
        for item in res:
            for j in range(len(item)):
                f.write(str(item[j]) + '\t')
            f.write('\n')
