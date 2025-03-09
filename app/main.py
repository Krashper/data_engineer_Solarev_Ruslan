import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from datetime import datetime
import random
from typing import List, Tuple
from scripts.agg_funcs import DataAggregation
from scripts.generate_data import DataGenerator



conn = psycopg2.connect(dbname="test_db", user="postgres", password="12345", host="db", port="5432")

# faker = Faker()


# def insert_users(users: List[Tuple]):
#     try:
#         sql = """INSERT INTO "Users" (username, password_hash, email, created_at)
#         VALUES %s"""
#         cur = conn.cursor()

#         execute_values(cur, sql, users)
#         conn.commit()
#     except Exception as e:
#         print("Ошибка:", e)
#         conn.close()


# def insert_actions(actions: List[Tuple]):
#     try:
#         sql = """INSERT INTO "Actions" (type)
#         VALUES %s"""
#         cur = conn.cursor()
#         execute_values(cur, sql, actions)
#         conn.commit()

#     except Exception as e:
#         print("Ошибка:", e)
#         conn.close()


# def generate_users(created_at):
#     num_users = random.choice(range(5, 11))

#     users = [(
#         faker.user_name(),
#         faker.sha256(),
#         faker.email(),
#         created_at)
#         for i in range(num_users)
#         ]

#     annonimus = [(None, None, None, created_at)
#         for i in range(num_users)]

#     users = users + annonimus

#     insert_users(users)


# actions = [
#     ("Первый заход на сайт",),
#     ("Регистрация",),
#     ("Логин",),
#     ("Логаут",),
#     ("Создание темы",),
#     ("Заход на тему",),
#     ("Удаление темы",),
#     ("Написание сообщения",)
# ]

# insert_actions(actions)


# def get_action_id(action):
#     action_id = pd.read_sql(f'''
#     SELECT action_id
#     FROM "Actions"
#     WHERE type = \'{action}\'''', conn)["action_id"].tolist()[0]

#     return action_id


# def user_is_logined(user_id: int):
#     user_info = pd.read_sql(f'''
#     SELECT * 
#     FROM "Users"
#     WHERE user_id = {user_id}''', conn)["username"].tolist()[0]

#     return user_info is not None


# def insert_topic(topic: Tuple):
#     try:
#         sql = """INSERT INTO "Content" (type, created_at)
#         VALUES (%s, %s)
#         RETURNING content_id;"""
#         cur = conn.cursor()
#         cur.execute(sql, ("Тема", topic[1]))

#         content_id = cur.fetchone()[0]

#         sql = """INSERT INTO "Topics"
#         VALUES (%s, %s);"""
#         cur.execute(sql, (content_id, topic[0]))
        
#         conn.commit()

#         return content_id
#     except Exception as e:
#         print("Ошибка:", e)
#         conn.close()


# def insert_message(message: Tuple):
#     try:
#         sql = """INSERT INTO "Content" (type, created_at)
#         VALUES (%s, %s)
#         RETURNING content_id;"""
#         cur = conn.cursor()
#         cur.execute(sql, ("Сообщение", message[2]))

#         content_id = cur.fetchone()[0]

#         sql = """INSERT INTO "Messages"
#         VALUES (%s, %s, %s);"""
#         cur.execute(sql, (content_id, message[1], message[0]))
        
#         conn.commit()

#         return content_id
#     except Exception as e:
#         print("Ошибка:", e)
#         conn.close()


# def insert_log(log: Tuple):
#     try:
#         sql = """INSERT INTO "Logs" (user_id, action_id, is_success, created_at)
#         VALUES (%s, %s, %s, %s)
#         RETURNING log_id;"""
#         cur = conn.cursor()
#         cur.execute(sql, log)

#         log_id = cur.fetchone()[0]
        
#         conn.commit()

#         return log_id
#     except Exception as e:
#         print("Ошибка:", e)
#         conn.close()


# def insert_content_log(log_id: int, content_id: int):
#     try:
#         sql = """INSERT INTO "Content_Log" (log_id, content_id)
#         VALUES (%s, %s);"""
#         cur = conn.cursor()
#         cur.execute(sql, (log_id, content_id))
#         conn.commit()
        
#     except Exception as e:
#         print("Ошибка:", e)
#         conn.close()


# actions = pd.read_sql('SELECT * FROM "Actions"', conn)["type"].tolist()

# for day in range(1, 32):
#     created_at = datetime(year=2025, month=1, day=day)

#     generate_users(created_at)

#     users_id = pd.read_sql('SELECT user_id FROM "Users"', conn)["user_id"].tolist()

#     for action in actions:
#         action_id = get_action_id(action)
#         num_actions = random.choice(range(5, 11))

#         for i in range(num_actions):
#             user_id = random.choice(users_id)
#             if action == "Создание темы":
#                 if user_is_logined(user_id):
#                     is_success = True
#                     topic_title = faker.sentence()
#                     content_id = insert_topic((topic_title, created_at))
#                     log_id = insert_log((user_id, action_id, is_success, created_at))
#                     insert_content_log(log_id, content_id)
#                 else:
#                     is_success = False
#                     insert_log((user_id, action_id, is_success, created_at))
            
#             elif action == "Написание сообщения":
#                 is_success = True
#                 topics_id = pd.read_sql('SELECT content_id FROM "Content" WHERE type = \'Тема\'', conn)["content_id"].tolist()
#                 topic_id = random.choice(topics_id)
#                 message_content = faker.sentence()
#                 content_id = insert_message((message_content, topic_id, created_at))
#                 log_id = insert_log((user_id, action_id, is_success, created_at))
#                 insert_content_log(log_id, content_id)
            
#             else:
#                 is_success = True
#                 insert_log((user_id, action_id, is_success, created_at))

generator = DataGenerator(conn)
generator.generate_data()

aggregator = DataAggregation(1, 31, conn)
aggregator.get_agg_file()

conn.close()