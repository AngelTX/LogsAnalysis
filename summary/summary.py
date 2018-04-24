import psycopg2
from datetime import datetime

# What are the most popular three articles of all time?
prompt1 = ("What are the most popular three articles of all time?")
question1 = ("""
                select articles.title, count(*) as views
                from articles inner join log on log.path
                like concat('%', articles.slug, '%')
                where log.status like '%200%' group by
                articles.title, log.path order by views desc limit 3
            """)

# Who are the most popular article authors of all time?
prompt2 = "Who are the most popular article authors of all time?"
question2 = ("""
                select authors.name, count(*) as views from articles inner
                join authors on articles.author = authors.id inner join log
                on log.path like concat('%', articles.slug, '%') where
                log.status like '%200%' group
                by authors.name order by views desc
            """)
# On which days did more than 1% of requests lead to errors
prompt3 = "On which days did more than 1% of requests lead to errors?"
question3 = ("""
                select day, perc from (
                select day, round((sum(requests)/(select count(*) from log where
                substring(cast(log.time as text), 0, 11) = day) * 100), 2) as
                perc from (select substring(cast(log.time as text), 0, 11) as day,
                count(*) as requests from log where status like '%404%' group by day)
                as log_percentage group by day order by perc desc) as final_query
                where perc >= 1
            """)

def connect(database_name="news"):
    try:
        db = psycopg2.connect("dbname={}".format(database_name))
        cursor = db.cursor()
        return db, cursor
    except:
        print ("Unable to connect.")

def get_query_results(query):
    #runs the connect function for the database
    db, cursor = connect()

    #passes the query as an argument to execute.
    cursor.execute(query)
    return cursor.fetchall()
    db.close()

def print_query_results(query_results):
    print query_results[1]
    for x in query_results[0]:
        print("\t{} - {} views".format(x[0], x[1]))

def print_errors(query_results):
    print mostErrors[1]
    d = datetime.strptime(query_results[0][0][0], '%Y-%m-%d')
    day_string = d.strftime('%B %d, %Y')
    print("\t{} -- {}% errors".format(day_string, query_results[0][0][1]))

popArticles = get_query_results(question1), prompt1
popAuthors = get_query_results(question2), prompt2
mostErrors = get_query_results(question3), prompt3

print_query_results(popArticles)
print_query_results(popAuthors)
print_errors(mostErrors)
