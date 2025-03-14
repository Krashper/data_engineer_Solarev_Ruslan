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
import os
from dotenv import load_dotenv


class Environment:
    """
    Класс для загрузки переменных окружения.
    """
    def __init__(self):
        load_dotenv()  # Загружаем переменные окружения из .env файла
        self.db_name = os.getenv('POSTGRES_DB')
        self.user = os.getenv('POSTGRES_USER')
        self.password = os.getenv('POSTGRES_PASSWORD')
        self.host = os.getenv('POSTGRES_HOST', 'db')  # По умолчанию 'db'
        self.port = os.getenv('POSTGRES_PORT', '5432')  # По умолчанию '5432'

    def get_connection(self):
        """
        Создает и возвращает соединение с базой данных.
        """
        return psycopg2.connect(
            dbname=self.db_name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )


def main():
    env = Environment()
    conn = env.get_connection()

    generator = DataGenerator(conn)
    generator.generate_data()

    aggregator = DataAggregation(1, 31, conn)
    aggregator.get_agg_file()

    conn.close()
    print("Соединение с базой данных закрыто")


if __name__ == "__main__":
    main()