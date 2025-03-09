import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from datetime import datetime
import random
from typing import List, Tuple
import json


class DataGenerator:
    def __init__(self, conn):
        self.conn = conn
        self.faker = Faker()

    def insert_users(self, users: List[Tuple]):
        try:
            sql = """INSERT INTO "Users" (username, password_hash, email, created_at)
            VALUES %s"""
            cur = self.conn.cursor()

            execute_values(cur, sql, users)
            self.conn.commit()
        except Exception as e:
            print("Ошибка:", e)
            self.conn.close()


    def insert_actions(self, actions: List[Tuple]):
        try:
            sql = """INSERT INTO "Actions" (type)
            VALUES %s"""
            print(actions)
            cur = self.conn.cursor()
            execute_values(cur, sql, actions)
            self.conn.commit()

        except Exception as e:
            print("Ошибка:", e)
            self.conn.close()


    def generate_users(self, created_at):
        num_users = random.choice(range(5, 11))

        users = [(
            self.faker.user_name(),
            self.faker.sha256(),
            self.faker.email(),
            created_at)
            for i in range(num_users)
            ]

        annonimus = [(None, None, None, created_at)
            for i in range(num_users)]

        users = users + annonimus

        self.insert_users(users)

    def get_action_id(self, action):
        action_id = pd.read_sql(f'''
        SELECT action_id
        FROM "Actions"
        WHERE type = \'{action}\'''', self.conn)["action_id"].tolist()[0]

        return action_id


    def user_is_logined(self, user_id: int):
        user_info = pd.read_sql(f'''
        SELECT * 
        FROM "Users"
        WHERE user_id = {user_id}''', self.conn)["username"].tolist()[0]

        return user_info is not None


    def insert_topic(self, topic: Tuple):
        try:
            sql = """INSERT INTO "Content" (type, created_at)
            VALUES (%s, %s)
            RETURNING content_id;"""
            cur = self.conn.cursor()
            cur.execute(sql, ("Тема", topic[1]))

            content_id = cur.fetchone()[0]

            sql = """INSERT INTO "Topics"
            VALUES (%s, %s);"""
            cur.execute(sql, (content_id, topic[0]))
            
            self.conn.commit()

            return content_id
        except Exception as e:
            print("Ошибка:", e)
            self.conn.close()


    def insert_message(self, message: Tuple):
        try:
            sql = """INSERT INTO "Content" (type, created_at)
            VALUES (%s, %s)
            RETURNING content_id;"""
            cur = self.conn.cursor()
            cur.execute(sql, ("Сообщение", message[2]))

            content_id = cur.fetchone()[0]

            sql = """INSERT INTO "Messages"
            VALUES (%s, %s, %s);"""
            cur.execute(sql, (content_id, message[1], message[0]))
            
            self.conn.commit()

            return content_id
        except Exception as e:
            print("Ошибка:", e)
            self.conn.close()


    def insert_log(self, log: Tuple):
        try:
            sql = """INSERT INTO "Logs" (user_id, action_id, is_success, created_at)
            VALUES (%s, %s, %s, %s)
            RETURNING log_id;"""
            cur = self.conn.cursor()
            cur.execute(sql, log)

            log_id = cur.fetchone()[0]
            
            self.conn.commit()

            return log_id
        except Exception as e:
            print("Ошибка:", e)
            self.conn.close()


    def insert_content_log(self, log_id: int, content_id: int):
        try:
            sql = """INSERT INTO "Content_Log" (log_id, content_id)
            VALUES (%s, %s);"""
            cur = self.conn.cursor()
            cur.execute(sql, (log_id, content_id))
            self.conn.commit()
            
        except Exception as e:
            print("Ошибка:", e)
            self.conn.close()

    def generate_data(self):
        with open('data/actions.json', 'r') as file:
            actions_json = json.load(file)
            actions = [(action["name"],) for action in actions_json["actions"]]

        self.insert_actions(actions)
        actions = pd.read_sql('SELECT * FROM "Actions"', self.conn)["type"].tolist()

        for day in range(1, 32):
            created_at = datetime(year=2025, month=1, day=day)

            self.generate_users(created_at)

            users_id = pd.read_sql('SELECT user_id FROM "Users"', self.conn)["user_id"].tolist()

            for action in actions:
                action_id = self.get_action_id(action)
                num_actions = random.choice(range(5, 11))

                for i in range(num_actions):
                    user_id = random.choice(users_id)
                    if action == "Создание темы":
                        if self.user_is_logined(user_id):
                            is_success = True
                            topic_title = self.faker.sentence()
                            content_id = self.insert_topic((topic_title, created_at))
                            log_id = self.insert_log((user_id, action_id, is_success, created_at))
                            self.insert_content_log(log_id, content_id)
                        else:
                            is_success = False
                            self.insert_log((user_id, action_id, is_success, created_at))
                    
                    elif action == "Написание сообщения":
                        is_success = True
                        topics_id = pd.read_sql('SELECT content_id FROM "Content" WHERE type = \'Тема\'', self.conn)["content_id"].tolist()
                        topic_id = random.choice(topics_id)
                        message_content = self.faker.sentence()
                        content_id = self.insert_message((message_content, topic_id, created_at))
                        log_id = self.insert_log((user_id, action_id, is_success, created_at))
                        self.insert_content_log(log_id, content_id)
                    
                    else:
                        is_success = True
                        self.insert_log((user_id, action_id, is_success, created_at))