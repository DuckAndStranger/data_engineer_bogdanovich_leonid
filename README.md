# Генерация и анализ логов форума

## Описание проекта

Этот проект представляет собой систему генерации и анализа данных для форума.

## Установка и запуск

1. Клонируйте репозиторий:
   ```bash
   git clone https://github.com/DuckAndStranger/data_engineer_bogdanovich_leonid.git
   ```

2. Перейдите в директорию проекта:
   ```bash
   cd data_engineer_bogdanovich_leonid
   ```

3. Установите необходимые зависимости:
   ```bash
   pip install -r requirements.txt
   ```

4. Запустите docker-compose
   ```bash
   docker-compose up
   ```

5. Дождитесь инициализации БД, а также конца генерации логов. Запустите скрипт агрегации данных с нужными датами. 
   Данные в БД с 1 января 2025 по 30 января 2025.
   ```bash
   python ./script.py --start_date 2025-01-01 --end_date 2025-01-10
   ```

## Конфигурация

   script.py принимает начальную и конечные даты а также названия файла, в который запишутся результаты.
   Пример:
   ```bash
   python3 script.py --start_date 2025-01-01 --end_date 2025-01-30 --output data.csv
   ```
