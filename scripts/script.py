from pyspark.sql import *

spark = SparkSession.builder.getOrCreate()

DATASET_PATH = 'hdfs:///datasets/reddit_data'

for month in range(6, 12):
    PATH = DATASET_PATH + '/2016/RC_2016-{num:02d}.bz2'.format(num=month)
    comments = spark.read.json(PATH)
    comments = comments.filter(comments['subreddit'] == 'politics').filter(comments['body'] != '[deleted]')
    comments = comments.select('author', 'author_flair_text', 'body', 'created_utc', 'gilded', 'id', 'link_id', 'parent_id', 'score')
    comments.write.mode('overwrite').parquet('politics_2016_{num:02d}'.format(num=month))
