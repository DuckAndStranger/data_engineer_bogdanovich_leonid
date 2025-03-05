import random
from datetime import datetime, timedelta
import psycopg2
from faker import Faker

class DataGenerator:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="forum",
            user="postgres",
            password="postgres",
            host="postgres",
            port="5432"
        )
        self.cur = self.conn.cursor()

        self.fake = Faker()
        # Константы для генерации
        self.MIN_ACTIONS_PER_TYPE = 5
        self.ANONYMOUS_COMMENT_RATIO = 0.5
        self.MIN_LOGIN_ERRORS = 2
        self.DAYS = 30
        
        # Типы активности
        self.ACTIVITY_TYPES = {
            'first_visit': 1,
            'registration': 2,
            'login': 3,
            'logout': 4,
            'create_topic': 5,
            'view_topic': 6,
            'delete_topic': 7,
            'create_comment': 8
        }

    def generate_cookie(self):
            return ''.join(random.choices('0123456789abcdef', k=32))

    def generate_users(self, count=1):
        user_ids = []
        for _ in range(count):
            self.cur.execute(
                "INSERT INTO users (name) VALUES (%s) RETURNING id",
                (self.fake.name(),)
            )
            user_ids.append(self.cur.fetchone()[0])
        self.conn.commit()
        return user_ids

    def generate_topics(self, user_id, count=1):
        topic_ids = []
        for _ in range(count):
            self.cur.execute(
                "INSERT INTO topics (name, user_id) VALUES (%s, %s) RETURNING id",
                (self.fake.sentence(nb_words=3), user_id)
            )
            topic_ids.append(self.cur.fetchone()[0])
        self.conn.commit()
        return topic_ids
    
    def generate_time(self, base_time, hour_shift=0, min_minutes=1, max_minutes=5):
            # Генерируем новое время с учетом ограничений текущего дня
            new_time = base_time + timedelta(
                hours=hour_shift,
                minutes=random.randint(min_minutes, max_minutes),
                seconds=random.randint(0, 59)
            )
            
            # Проверяем, не вышли ли за пределы текущего дня
            end_of_day = base_time.replace(hour=23, minute=59, second=59)
            return min(new_time, end_of_day)
    
    def generate_first_visit(self, date):
            # Первый визит происходит в первые 12 часов дня
            time = self.generate_time(date, hour_shift=random.randint(0, 12))
            
            # Генерируем cookie для нового пользователя
            cookie = self.generate_cookie()
            
            # Первый заход
            self.cur.execute("""
                INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (time, None, self.ACTIVITY_TYPES['first_visit'], None, 200, cookie))

            return cookie, time

    def generate_daily_logs(self, date, user_ids, topic_ids, comment_ids, user_cookies, logged_users):
        # Словарь для хранения времени последнего действия пользователя
        user_last_action = {user_id: date for user_id in user_ids}
        last_anonymous_time = date  # Для отслеживания времени анонимных пользователей
        
        # 1. Первый заход и регистрация (для новых пользователей)
        new_users_count = random.randint(self.MIN_ACTIONS_PER_TYPE, self.MIN_ACTIONS_PER_TYPE + 10)
        for _ in range(new_users_count):
            cookie, time_reg = self.generate_first_visit(date)
            # Регистрация происходит через 2-5 минут после первого захода
            registration_time = self.generate_time(time_reg, min_minutes=2, max_minutes=5)
            
            # Регистрация (используем тот же cookie)
            last_user_id = self.generate_users()[0]
            user_cookies[last_user_id] = cookie
            user_ids.append(last_user_id)
            self.cur.execute("""
                INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (registration_time, last_user_id, self.ACTIVITY_TYPES['registration'], None, 200, cookie))
            
            user_last_action[last_user_id] = registration_time

        # 2. Логин
        login_count = random.randint(self.MIN_ACTIONS_PER_TYPE, self.MIN_ACTIONS_PER_TYPE + 8)
        available_for_login = list(set(user_ids) - set(logged_users))
        if login_count > len(available_for_login):
            login_count = len(available_for_login)
        for _ in range(login_count):
            user_id = random.choice(available_for_login)
            available_for_login.remove(user_id)
            # Логин происходит через 2-10 минут после последнего действия
            login_time = self.generate_time(user_last_action[user_id], hour_shift=random.randint(5, 10), min_minutes=2, max_minutes=10)
            
            # Получаем существующий cookie или генерируем новый
            if user_id not in user_cookies:
                user_cookies[user_id] = self.generate_cookie()
            cookie = user_cookies[user_id]
            
            # Логин
            self.cur.execute("""
                INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (login_time, user_id, self.ACTIVITY_TYPES['login'], None, 200, cookie))
            user_last_action[user_id] = login_time
            logged_users.append(user_id)

        # 3. Создание тем
        for _ in range(self.MIN_LOGIN_ERRORS):
            available_users = list(set(user_ids) - set(logged_users))
            if available_users:
                user_id = random.choice(available_users)
                time = self.generate_time(user_last_action[user_id], min_minutes=5, max_minutes=15)
                cookie = self.generate_cookie()
                self.cur.execute("""
                    INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (time, user_id, self.ACTIVITY_TYPES['create_topic'], None, 403, cookie))
            else:
                cookie, time = self.generate_first_visit(date)
                self.cur.execute("""
                    INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (time, None, self.ACTIVITY_TYPES['create_topic'], None, 403, cookie))

        topic_create_count = random.randint(self.MIN_ACTIONS_PER_TYPE+2, self.MIN_ACTIONS_PER_TYPE+12)
        for _ in range(topic_create_count):
            user_id = random.choice(logged_users)
            time = self.generate_time(user_last_action[user_id], hour_shift=random.randint(0, 3), min_minutes=5, max_minutes=15)
            cookie = user_cookies[user_id]
            topic_id = self.generate_topics(user_id)[0]
            self.cur.execute("""
                INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (time, user_id, self.ACTIVITY_TYPES['create_topic'], topic_id, 200, cookie))
            user_last_action[user_id] = time
            topic_ids.append(topic_id)

        # 4. Просмотр тем и создание комментариев
        view_and_comment_count = random.randint(self.MIN_ACTIONS_PER_TYPE, self.MIN_ACTIONS_PER_TYPE + 10)
        for i in range(view_and_comment_count):
            is_anonymous = random.random() > self.ANONYMOUS_COMMENT_RATIO
            user_id = None if is_anonymous else random.choice(logged_users)
            
            # Просмотры темы
            topic_id = random.choice(topic_ids)
            if user_id:
                time = self.generate_time(user_last_action[user_id], hour_shift=random.randint(0, 3), min_minutes=3, max_minutes=10)
                cookie = user_cookies[user_id]
            else:
                time = self.generate_time(last_anonymous_time, hour_shift=random.randint(0, 3), min_minutes=1, max_minutes=5)
                cookie = self.generate_cookie()
                last_anonymous_time = time
            
            self.cur.execute("""
                INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (time, user_id, self.ACTIVITY_TYPES['view_topic'], topic_id, 200, cookie))               
            if user_id:
                user_last_action[user_id] = time

            # Комментарии создаются через 2-7 минут после просмотра
            comment_time = self.generate_time(time, min_minutes=10, max_minutes=20)
            comment_text = self.fake.text(max_nb_chars=200)

            if random.random() < 0.5 or topic_id not in comment_ids:
                self.cur.execute("""
                    INSERT INTO comments (user_id, topic_id, text)
                    VALUES (%s, %s, %s)
                    RETURNING id
                 """, (user_id, topic_id, comment_text))
                comment_ids[topic_id] = [self.cur.fetchone()[0]]
                extra = 'topic'
                self.cur.execute("""
                    INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie, extra)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (comment_time, user_id, self.ACTIVITY_TYPES['create_comment'], topic_id, 200, cookie, extra))
            else:
                comment_id = random.choice(comment_ids[topic_id])
                self.cur.execute("""
                    INSERT INTO comments (user_id, topic_id, parent_id, text)
                    VALUES (%s, %s, %s, %s) 
                    RETURNING id
                """, (user_id, topic_id, comment_id, comment_text))
                comment_ids[topic_id].append(self.cur.fetchone()[0])
                extra = 'comment'
                self.cur.execute("""
                    INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie, extra)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (comment_time, user_id, self.ACTIVITY_TYPES['create_comment'], comment_id, 200, cookie, extra))
            if user_id:
                user_last_action[user_id] = comment_time

        # 5. Удаление тем
        delete_topic_count = random.randint(self.MIN_ACTIONS_PER_TYPE, self.MIN_ACTIONS_PER_TYPE + 2)
        for _ in range(delete_topic_count):
            user_id = random.choice(logged_users)
            time = self.generate_time(user_last_action[user_id], min_minutes=5, max_minutes=15)
            cookie = user_cookies[user_id]
            topic_id = random.choice(topic_ids)
            self.cur.execute("""
                INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (time, user_id, self.ACTIVITY_TYPES['delete_topic'], topic_id, 200, cookie))
            user_last_action[user_id] = time
            topic_ids.remove(topic_id)

        # 6. Выход
        logout_count = random.randint(self.MIN_ACTIONS_PER_TYPE, self.MIN_ACTIONS_PER_TYPE + 2)
        for _ in range(logout_count):
            user_id = random.choice(logged_users)
            time = user_last_action[user_id] + timedelta(minutes=random.randint(5, 10))
            self.cur.execute("""
                INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (time, user_id, self.ACTIVITY_TYPES['logout'], None, 200, cookie))
            user_last_action[user_id] = time
            logged_users.remove(user_id)
        
        self.conn.commit()

    def generate_month_data(self, year, month):
        user_ids = []
        topic_ids = []
        user_cookies = {}
        comment_ids = {}
        logged_users = []

        # Генерируем данные за каждый день
        start_date = datetime(year, month, 1)
        for day in range(self.DAYS):
            current_date = start_date + timedelta(days=day)
            self.generate_daily_logs(current_date, user_ids, topic_ids, comment_ids, user_cookies, logged_users)

    def cleanup(self):
        self.cur.close()
        self.conn.close()

if __name__ == "__main__":
    generator = DataGenerator()
    try:
        generator.generate_month_data(2025, 1)
        print("Данные успешно сгенерированы")
    finally:
        generator.cleanup()