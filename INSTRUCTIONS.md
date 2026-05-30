# Инструкция по запуску Flink Streaming Pipeline (Лабораторная работа 3)

Скрипт потоковой обработки считывает топик Kafka, преобразует сообщения в модель «звезда» и записывает результат в базу данных PostgreSQL в режиме реального времени.

## Шаги для запуска:

1. **Остановка предыдущей лабораторной работы (если запущена):**
   Перед запуском обязательно освободите порты, остановив предыдущие контейнеры:
   ```bash
   # Выполнить в папке BigDataSpark (если запущена)
   docker-compose down
   ```

2. **Запуск Docker-окружения:**
   ```bash
   # Выполнить в папке BigDataFlink
   docker-compose up -d
   ```
   *Контейнер `producer` автоматически запустится, установит зависимости, подождет готовности Kafka и начнет транслировать данные из CSV-файлов в топик.*

3. **Запуск Flink Streaming Job:**
   Отправьте Flink-задачу на выполнение в JobManager:
   ```bash
   docker exec -it bigdataflink-jobmanager-1 flink run -py /opt/flink/flink_job.py
   ```

4. **Проверка результатов:**
   Вы можете открыть веб-интерфейс Flink по адресу `http://localhost:8081` для мониторинга выполнения задачи.

   Чтобы проверить поступление данных в PostgreSQL (порт `5432`, БД `analytics`, пользователь `user`, пароль `password`):
   ```bash
   docker exec -it bigdataflink-postgres-1 psql -U user -d analytics -c "
   SELECT 'dim_customer' as table_name, count(*) from dim_customer
   UNION ALL
   SELECT 'dim_pet', count(*) from dim_pet
   UNION ALL
   SELECT 'dim_seller', count(*) from dim_seller
   UNION ALL
   SELECT 'dim_supplier', count(*) from dim_supplier
   UNION ALL
   SELECT 'dim_store', count(*) from dim_store
   UNION ALL
   SELECT 'dim_product', count(*) from dim_product
   UNION ALL
   SELECT 'fact_sales', count(*) from fact_sales;
   "
   ```
