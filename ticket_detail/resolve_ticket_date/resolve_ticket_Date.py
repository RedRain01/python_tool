import os
import mysql.connector


def insert_into_doris(records):
    if not records:
        return
    try:
        conn = mysql.connector.connect(
            host="192.168.0.104",
            port=9030,
            user="root",
            password="why123",
            database="demo"
        )
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO ticket_date11 (order_date, order_code)
        VALUES (%s, %s)
        """

        cursor.executemany(insert_query, records)
        conn.commit()
        print(f"Inserted {cursor.rowcount} records into ticket_date11.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        conn.close()


def parse_and_store(base_path):
    for folder in os.listdir(base_path):
        folder_path = os.path.join(base_path, folder)

        if os.path.isdir(folder_path) and not folder.isdigit():  # 确保是日期格式的文件夹
            records = []
            for file in os.listdir(folder_path):
                if file.endswith(".csv"):
                    file_prefix = os.path.splitext(file)[0]  # 去掉 .csv 后缀
                    records.append((folder, file_prefix))

            if records:
                insert_into_doris(records)
            else:
                print(f"No records found in {folder}.")


if __name__ == "__main__":
    base_path = "/mnt/data3/ticket_date/2025/newcsv111"  # A 文件夹路径
    parse_and_store(base_path)