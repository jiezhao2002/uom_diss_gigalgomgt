import pandas as pd
import praw
import time

reddit = praw.Reddit(
    client_id="MASKED-AS-PRIVATE-ID/PASSWORD",
    client_secret=" MASKED-AS-PRIVATE-ID/PASSWORD ",
    user_agent="upwork-crawler"
)
sleep_time = 2
# temporary storage constructioj
post_title = []
post_id = []
post_datetime = []
post_score = []
post_body = []
comment_id = []
comment_text = []
comment_datetime = []
comment_score = []
# df = pd.read_csv("raw_dataMar28.csv")
seen_ids = set()

total_fetched = 0
target_posts = 115
start_time = time.time()
# keywords: strategy; hot posts.
posts = reddit.subreddit("Upwork").search("strategy",sort="comments",limit=200)
# posts = reddit.subreddit("Upwork").hot(limit=200)
## time.time() <= start_time + 3600
while total_fetched<=target_posts:
    for post in posts:
        if post.id not in seen_ids and post.num_comments > 10:
            print(f"crawling comments from post {post.title}")
            seen_ids.add(post.id)
            total_fetched += 1
            try:
                post.comments.replace_more(limit=None)
                comment_queue = post.comments[:]
                while comment_queue:
                    comment = comment_queue.pop(0)
                    post_title.append(post.title)
                    post_id.append(post.id)
                    post_datetime.append(post.created_utc)
                    post_score.append(post.score)
                    post_body.append(post.selftext)
                    comment_id.append(comment.id)
                    comment_text.append(comment.body)
                    comment_datetime.append(comment.created_utc)
                    comment_score.append(comment.score)
                    comment_queue.extend(comment.replies)
            except (Exception, KeyError, AttributeError):
                continue
        if total_fetched >= target_posts: break
# data export to csv file
raw_data = {
    "post_title": post_title,
    "post_id": post_id,
    "post_body": post_body,
    "post_datetime": post_datetime,
    "post_score": post_score,
    "comment_id": comment_id,
    "comment_text": comment_text,
    "comment_datetime": comment_datetime,
    "comment_score": comment_score
}
# mode="a" for append incremented data
export_df = pd.DataFrame(raw_data).drop_duplicates(subset=["comment_id"],keep="first")
export_df.to_csv("raw_dataApr7.csv",index=False)

