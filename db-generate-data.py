import random
from datetime import datetime, timedelta
import psycopg2
from faker import Faker


class DataGenerator:
    def __init__(
            self,
            dbname,
            postgres_user,
            postgres_password,
            postgres_host,
            postgres_port):
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=postgres_user,
            password=postgres_password,
            host=postgres_host,
            port=postgres_port
        )

        self.cur = self.conn.cursor()
        self.fake = Faker()

        self.MIN_ACTIONS_PER_TYPE = 5
        self.ANONYMOUS_COMMENT_RATIO = 0.5
        self.MIN_LOGIN_ERRORS = 2
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
        self.user_ids = []
        self.topic_ids = []
        self.user_cookies = {}
        self.comment_ids = {}
        self.logged_users = []

        random.seed(datetime.now().timestamp())

    def get_count_config(self):
        return {
            'registration_count': random.randint(
                self.MIN_ACTIONS_PER_TYPE + 2,
                self.MIN_ACTIONS_PER_TYPE + 10
            ),
            'login_count': random.randint(
                self.MIN_ACTIONS_PER_TYPE + 1,
                self.MIN_ACTIONS_PER_TYPE + 3
            ),
            'topic_create_count': random.randint(
                self.MIN_ACTIONS_PER_TYPE + 2,
                self.MIN_ACTIONS_PER_TYPE + 12
            ),
            'activity_count': random.randint(
                self.MIN_ACTIONS_PER_TYPE + 30,
                self.MIN_ACTIONS_PER_TYPE + 50
            ),
            'delete_topic_count': random.randint(
                self.MIN_ACTIONS_PER_TYPE,
                self.MIN_ACTIONS_PER_TYPE + 2
            ),
            'logout_count': random.randint(
                self.MIN_ACTIONS_PER_TYPE,
                self.MIN_ACTIONS_PER_TYPE + 3),
            'error_count': random.randint(
                self.MIN_LOGIN_ERRORS,
                self.MIN_LOGIN_ERRORS + 3)
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

    def generate_time(
            self,
            base_time,
            hour_shift=0,
            min_minutes=1,
            max_minutes=5):
        new_time = base_time + timedelta(
            hours=hour_shift,
            minutes=random.randint(min_minutes, max_minutes),
            seconds=random.randint(0, 59)
        )
        end_of_day = base_time.replace(hour=23, minute=59, second=59)
        return min(new_time, end_of_day)

    def generate_first_visit(self, date):
        time = self.generate_time(
            date, 
            hour_shift=random.randint(6, 16))
        cookie = self.generate_cookie()
        self.cur.execute("""
            INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (time, None, self.ACTIVITY_TYPES['first_visit'], None, 200, cookie))
        return cookie, time

    def generate_registration(
            self,
            date,
            user_ids,
            user_cookies,
            user_last_action,
            count):
        for _ in range(count):
            cookie, time_reg = self.generate_first_visit(date)
            registration_time = self.generate_time(
                time_reg, 
                min_minutes=2, 
                max_minutes=5)
            last_user_id = self.generate_users()[0]
            user_cookies[last_user_id] = cookie
            user_ids.append(last_user_id)
            self.cur.execute("""
                INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (registration_time, last_user_id, self.ACTIVITY_TYPES['registration'], None, 201, cookie))
            user_last_action[last_user_id] = registration_time

    def generate_login(
            self,
            user_cookies,
            user_last_action,
            user_ids,
            login_count,
            logged_users):
        available_for_login = list(set(user_ids) - set(logged_users))
        if not available_for_login:
            print("Нет доступных пользователей для входа в систему")
            return

        for _ in range(login_count):
            if not available_for_login:
                break
            user_id = random.choice(available_for_login)
            available_for_login.remove(user_id)
            login_time = self.generate_time(
                user_last_action[user_id], 
                hour_shift=random.randint(5, 10), 
                min_minutes=2, 
                max_minutes=10)
            if user_id not in user_cookies:
                user_cookies[user_id] = self.generate_cookie()
            cookie = user_cookies[user_id]
            self.cur.execute("""
                INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (login_time, user_id, self.ACTIVITY_TYPES['login'], None, 200, cookie))
            user_last_action[user_id] = login_time
            logged_users.append(user_id)

    def generate_create_topic_with_error(
            self,
            date,
            user_ids,
            user_cookies,
            user_last_action,
            logged_users,
            count):
        for _ in range(count):
            available_users = list(set(user_ids) - set(logged_users))
            if available_users:
                user_id = random.choice(available_users)
                time = self.generate_time(
                    user_last_action[user_id], 
                    min_minutes=5, 
                    max_minutes=15)
                if user_id not in user_cookies:
                    user_cookies[user_id] = self.generate_cookie()
                cookie = user_cookies[user_id]
                self.cur.execute("""
                    INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (time, user_id, self.ACTIVITY_TYPES['create_topic'], None, 401, cookie))
            else:
                cookie, time = self.generate_first_visit(date)
                self.cur.execute("""
                    INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (time, None, self.ACTIVITY_TYPES['create_topic'], None, 401, cookie))

    def generate_create_topic(
            self,
            user_cookies,
            user_last_action,
            topic_ids,
            count,
            logged_users):
        if not logged_users:
            print("Нет авторизованных пользователей для создания топиков")
            return

        for _ in range(count):
            if not logged_users:
                break
            user_id = random.choice(logged_users)
            time = self.generate_time(
                user_last_action[user_id], 
                hour_shift=random.randint(0, 3), 
                min_minutes=5, 
                max_minutes=15)
            cookie = user_cookies[user_id]
            topic_id = self.generate_topics(user_id)[0]
            self.cur.execute("""
                INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (time, user_id, self.ACTIVITY_TYPES['create_topic'], topic_id, 201, cookie))
            user_last_action[user_id] = time
            topic_ids.append(topic_id)

    def generate_activity(
            self,
            date,
            user_cookies,
            user_last_action,
            topic_ids,
            count,
            logged_users,
            comment_ids):
        if not topic_ids:
            print("Нет доступных топиков для генерации активности")
            return
        
        for _ in range(count):
            is_anonymous = random.random() > self.ANONYMOUS_COMMENT_RATIO
            user_id = None if is_anonymous else random.choice(logged_users)
            topic_id = random.choice(topic_ids)

            if user_id:
                time = self.generate_time(
                    user_last_action[user_id], 
                    hour_shift=random.randint(0, 3), 
                    min_minutes=3, max_minutes=10)
                cookie = user_cookies[user_id]
            else:
                cookie, anonymous_login_time = self.generate_first_visit(date)
                time = self.generate_time(
                    anonymous_login_time, 
                    hour_shift=random.randint(0, 3), 
                    min_minutes=1, 
                    max_minutes=5)
                
            self.cur.execute("""
                INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (time, user_id, self.ACTIVITY_TYPES['view_topic'], topic_id, 200, cookie))

            if user_id:
                user_last_action[user_id] = time
            comment_time = self.generate_time(time, min_minutes=10, max_minutes=20)
            comment_text = self.fake.text(max_nb_chars=200)
            if topic_id not in comment_ids:
                comment_ids[topic_id] = []

            if random.random() < 0.5 or comment_ids[topic_id] == []:
                self.cur.execute("""
                    INSERT INTO comments (user_id, topic_id, text)
                    VALUES (%s, %s, %s)
                    RETURNING id
                 """, (user_id, topic_id, comment_text))
                extra = 'topic'
            else:
                comment_id = random.choice(comment_ids[topic_id])
                self.cur.execute("""
                    INSERT INTO comments (user_id, topic_id, parent_id, text)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (user_id, topic_id, comment_id, comment_text))
                extra = 'comment'

            comment_ids[topic_id].append(self.cur.fetchone()[0])
            self.cur.execute("""
                INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie, extra)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (comment_time, user_id, self.ACTIVITY_TYPES['create_comment'], topic_id, 201, cookie, extra))
            if user_id:
                user_last_action[user_id] = comment_time

    def generate_delete_topic(
            self,
            user_cookies,
            user_last_action,
            topic_ids,
            count,
            logged_users):
        if not logged_users or not topic_ids:
            print("Нет авторизованных пользователей или топиков для удаления")
            return

        for _ in range(count):
            if not logged_users or not topic_ids:
                break
            user_id = random.choice(logged_users)
            time = self.generate_time(
                user_last_action[user_id],
                min_minutes=5,
                max_minutes=15)
            cookie = user_cookies[user_id]
            topic_id = random.choice(topic_ids)
            self.cur.execute("""
                INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (time, user_id, self.ACTIVITY_TYPES['delete_topic'], topic_id, 204, cookie))
            user_last_action[user_id] = time
            topic_ids.remove(topic_id)

    def generate_logout(
            self,
            user_cookies,
            user_last_action,
            logged_users,
            count):
        if not logged_users:
            print("Нет авторизованных пользователей для выхода из системы")
            return

        for _ in range(count):
            if not logged_users:
                break
            user_id = random.choice(logged_users)
            time = user_last_action[user_id] + timedelta(minutes=random.randint(5, 10))
            cookie = user_cookies[user_id]
            self.cur.execute("""
                INSERT INTO logs (time, user_id, activity_type, activity_id, server_response, cookie)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (time, user_id, self.ACTIVITY_TYPES['logout'], None, 200, cookie))
            user_last_action[user_id] = time
            logged_users.remove(user_id)

    def generate_daily_logs(
            self,
            date,
            user_ids,
            topic_ids,
            comment_ids,
            user_cookies,
            logged_users):
        user_last_action = {user_id: date for user_id in user_ids}

        self.generate_registration(
            date,
            user_ids,
            user_cookies,
            user_last_action,
            self.get_count_config()['registration_count'])
        
        self.generate_login(
            user_cookies,
            user_last_action,
            user_ids,
            self.get_count_config()['login_count'],
            logged_users)
        
        self.generate_create_topic_with_error(
            date,
            user_ids,
            user_cookies,
            user_last_action,
            logged_users,
            self.get_count_config()['error_count'])
        
        self.generate_create_topic(
            user_cookies,
            user_last_action,
            topic_ids,
            self.get_count_config()['topic_create_count'],
            logged_users)
        
        self.generate_activity(
            date,
            user_cookies,
            user_last_action,
            topic_ids,
            self.get_count_config()['activity_count'],
            logged_users,
            comment_ids)
        
        self.generate_delete_topic(
            user_cookies,
            user_last_action,
            topic_ids,
            self.get_count_config()['delete_topic_count'],
            logged_users)
        
        self.generate_logout(
            user_cookies,
            user_last_action,
            logged_users,
            self.get_count_config()['logout_count'])
        
        self.conn.commit()

    def generate_month_data(self, year, month):
        start_date = datetime(year, month, 1)
        for day in range(30):
            current_date = start_date + timedelta(days=day)
            self.generate_daily_logs(
                current_date,
                self.user_ids,
                self.topic_ids,
                self.comment_ids,
                self.user_cookies,
                self.logged_users)

    def cleanup(self):
        self.cur.close()
        self.conn.close()


if __name__ == "__main__":
    generator = DataGenerator(
        dbname="forum",
        postgres_user="postgres",
        postgres_password="postgres",
        postgres_host="postgres",
        postgres_port="5432"
    )
    try:
        generator.generate_month_data(2025, 1)
        print("Данные успешно сгенерированы")
    except Exception as e:
        print(f"Ошибка при генерации данных: {e}")
    finally:
        generator.cleanup()
