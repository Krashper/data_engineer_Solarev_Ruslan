import pandas as pd


class DataAggregation:
    def __init__(self, min_day: int, max_day: int, conn):
        self.min_day = min_day
        self.max_day = max_day
        self.conn = conn
    
    def get_new_accounts_count(self):
        new_accounts_count = pd.read_sql('''
        SELECT EXTRACT(DAY FROM created_at) AS p_day, COUNT(*)
        FROM "Users"
        GROUP BY p_day
        ORDER BY p_day;''', con=self.conn)

        return new_accounts_count
    
    def get_message_anonymous_percentage(self, day: int):
        message_users = pd.read_sql(f'''
        SELECT u.*
        FROM "Content" AS co
        JOIN "Content_Log" AS cl
            ON cl.content_id = co.content_id
        JOIN "Logs" AS l
            ON l.log_id = cl.log_id
        JOIN "Users" AS u
            ON l.user_id = u.user_id
        WHERE co.type = 'Сообщение' AND EXTRACT(day FROM co.created_at) = {day};''', con=self.conn)
        
        percentage = round(
            len(message_users[message_users["username"].isnull()]) / max(len(message_users), 1) * 100, 
            1)

        return percentage

    def get_new_messages_count(self):
        new_messages_count = pd.read_sql('''
        SELECT EXTRACT(DAY FROM created_at) AS p_day, COUNT(*)
        FROM "Content" co
        WHERE co.type = 'Сообщение'
        GROUP BY p_day
        ORDER BY p_day;''', con=self.conn)

        return new_messages_count
    
    def get_new_topics_percentage(self, day: int):
        prev_days_topics_count = pd.read_sql(f'''
        SELECT COUNT(*)
        FROM "Content" co
        WHERE co.type = 'Тема' AND EXTRACT(DAY FROM created_at) < {day};''', con=self.conn)

        cur_day_topics_count = pd.read_sql(f'''
        SELECT COUNT(*)
        FROM "Content" co
        WHERE co.type = 'Тема' AND EXTRACT(DAY FROM created_at) = {day};''', con=self.conn)

        percentage = round(
            cur_day_topics_count["count"].iloc[0] / max(prev_days_topics_count["count"].iloc[0], 1) * 100, 
            1)

        return percentage

    def get_agg_file(self):
        data = pd.DataFrame({"day": range(self.min_day, self.max_day + 1)})

        data["new_accounts"] = self.get_new_accounts_count()["count"].iloc[self.min_day-1:self.max_day-1]
        data["new_messages"] = self.get_new_messages_count()["count"].iloc[self.min_day-1:self.max_day-1]
        data["anonymous_messages_pct"] = data["day"].apply(self.get_message_anonymous_percentage)
        data["new_topics_pct"] = data["day"].apply(self.get_new_topics_percentage)

        data.to_csv("data/agg_data.csv", index=False)