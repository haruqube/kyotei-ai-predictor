"""X(Twitter)投稿"""

import tweepy
from config import X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET


class XPoster:
    """Tweepyを使ったX投稿"""

    def __init__(self):
        self.client = None
        if all([X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET]):
            self.client = tweepy.Client(
                consumer_key=X_API_KEY,
                consumer_secret=X_API_SECRET,
                access_token=X_ACCESS_TOKEN,
                access_token_secret=X_ACCESS_SECRET,
            )

    def post(self, text: str) -> dict | None:
        """テキストをXに投稿"""
        if not self.client:
            print("[X] API未設定。投稿スキップ。")
            print(f"[X] 投稿内容:\n{text}")
            return None

        if len(text) > 280:
            text = text[:277] + "..."

        response = self.client.create_tweet(text=text)
        tweet_id = response.data["id"]
        print(f"[X] 投稿完了: https://x.com/i/status/{tweet_id}")
        return response.data
