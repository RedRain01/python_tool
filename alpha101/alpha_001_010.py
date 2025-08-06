from ticket_base_data.doris_connect.dorise_db import doris_db

def main():
    rows = doris_db.execute_query("SELECT *  FROM wdt_index_daily limit 1000", None, False)
    print(rows)


if __name__ == '__main__':
    main()