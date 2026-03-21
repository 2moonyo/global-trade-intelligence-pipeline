from pathlib import Path

import duckdb


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    db_path = project_root / 'warehouse' / 'analytics.duckdb'
    sql_path = project_root / 'warehouse' / 'bootstrap_silver_to_duckdb.sql'

    sql_text = sql_path.read_text(encoding='utf-8')

    with duckdb.connect(str(db_path)) as con:
        con.execute(sql_text)
        loaded = con.execute(
            """
            select table_name
            from information_schema.tables
            where table_schema = 'raw'
            order by table_name
            """
        ).fetchall()

    print(f'Warehouse ready at: {db_path}')
    print('Loaded raw tables:')
    for (table_name,) in loaded:
        print(f'- raw.{table_name}')


if __name__ == '__main__':
    main()
