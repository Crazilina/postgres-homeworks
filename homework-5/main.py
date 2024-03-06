import json

import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE {db_name}")
    cur.close()
    conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, 'r') as file:
        sql_script = file.read()
        cur.execute(sql_script)


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    cur.execute("""
            CREATE TABLE IF NOT EXISTS suppliers (
                supplier_id SERIAL PRIMARY KEY,
                company_name TEXT NOT NULL,
                contact TEXT,
                address TEXT,
                phone TEXT,
                fax TEXT,
                homepage TEXT
            );
        """)


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file, 'r') as file:
        data = json.load(file)
    return data


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    for supplier in suppliers:
        cur.execute(
            "INSERT INTO suppliers "
            "(company_name, contact, address, phone, fax, homepage) "
            "VALUES (%s, %s, %s, %s, %s, %s);", (
                supplier['company_name'],
                supplier['contact'],
                supplier['address'],
                supplier['phone'],
                supplier['fax'],
                supplier['homepage'],
            )
        )


def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    # Добавляем столбец supplier_id в таблицу products
    cur.execute("""
            ALTER TABLE products
            ADD COLUMN supplier_id INTEGER;
        """)

    # Загружаем данные о поставщиках из JSON файла
    with open(json_file, 'r') as file:
        suppliers_data = json.load(file)

    # Для каждого поставщика устанавливаем supplier_id для соответствующих продуктов
    for supplier in suppliers_data:
        # Находим supplier_id по имени компании
        cur.execute("""
                SELECT supplier_id FROM suppliers WHERE company_name = %s;
            """, (supplier['company_name'],))
        supplier_id_result = cur.fetchone()

        if supplier_id_result:
            supplier_id = supplier_id_result[0]
            # Устанавливаем supplier_id для всех продуктов, указанных поставщиком
            for product_name in supplier['products']:
                # Предполагается, что у вас есть столбец product_name в таблице products для идентификации продукта
                cur.execute("""
                        UPDATE products SET supplier_id = %s WHERE product_name = %s;
                    """, (supplier_id, product_name))

    # Устанавливаем внешний ключ только после обновления всех supplier_id
    cur.execute("""
            ALTER TABLE products
            ADD CONSTRAINT fk_products_supplier
            FOREIGN KEY (supplier_id)
            REFERENCES suppliers(supplier_id)
            ON DELETE SET NULL;
        """)


if __name__ == '__main__':
    main()
