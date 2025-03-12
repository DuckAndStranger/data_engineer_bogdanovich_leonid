import pandas as pd
import psycopg2
from psycopg2 import sql
import argparse
from datetime import datetime


def extract_logs(
        postgres_url,
        postgres_user,
        postgres_password,
        postgres_db,
        start_date,
        end_date):
    try:
        conn = psycopg2.connect(
            host=postgres_url,
            database=postgres_db,
            user=postgres_user,
            password=postgres_password
        )

        query = sql.SQL("SELECT * FROM logs WHERE DATE(time) BETWEEN %s AND %s")
        data = pd.read_sql_query(
            query.as_string(conn),
            conn,
            params=(start_date, end_date)
        )
        return data

    finally:
        if 'conn' in locals():
            conn.close()


def transform_data(data):
    data['day'] = data['time'].dt.date
    unique_days = data['day'].unique()
    final_data = pd.DataFrame()
    registrations = data[data['activity_type'] == 2].groupby('day')['user_id'].count()
    final_data['number_of_new_users'] = registrations
    anonymous_comments = data[(data['activity_type'] == 8) & (data['user_id'].isnull())].groupby('day')['id'].count()
    all_comments = data[data['activity_type'] == 8].groupby('day')['id'].count()
    final_data['anonymous_comments_ratio'] = round(anonymous_comments / all_comments, 2)
    final_data['comments_count'] = all_comments
    topic_count = data[(data['activity_type'] == 5) & (data['server_response'] != 401)].groupby('day')['id'].count() - data[data['activity_type'] == 7].groupby('day')['id'].count()
    accumulated_topic_count = topic_count.cumsum()
    topic_count_change = round(accumulated_topic_count.pct_change() * 100, 2)
    final_data['topic_count_change'] = topic_count_change
    final_data['date'] = unique_days
    return final_data[['date',
                       'number_of_new_users',
                       'anonymous_comments_ratio',
                       'comments_count',
                       'topic_count_change']]


def save_data_to_csv(data, filename):
    data.to_csv(filename, index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Извлечение и анализ данных из базы форума')
    parser.add_argument('--start_date', type=str, default='2025-01-01', 
                       help='Дата начала периода в формате YYYY-MM-DD')
    parser.add_argument('--end_date', type=str, default='2025-01-10', 
                       help='Дата окончания периода в формате YYYY-MM-DD')
    parser.add_argument('--output', type=str, default='data.csv',
                       help='Имя выходного файла')
    args = parser.parse_args()

    try:
        # Проверка корректности формата дат
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        
        if end_date < start_date:
            raise ValueError("Дата окончания не может быть раньше даты начала")

        data = extract_logs(
            'localhost',
            'postgres',
            'postgres',
            'forum',
            args.start_date,
            args.end_date)
        
        data = transform_data(data)
        save_data_to_csv(data, args.output)
        print(f"Данные успешно сохранены в файл {args.output}")
        print(f"Период: с {args.start_date} по {args.end_date}")
        
    except ValueError as e:
        print(f"Ошибка в формате дат: {e}")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
