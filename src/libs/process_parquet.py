from pathlib import Path

import duckdb


def output_parquet(arrow_data, output_dir: str):
    """
    与えられたArrow TableをParquetとして出力する（year, monthでパーティショニング）。
    """
    try:
        duckdb.register("merged_data", arrow_data)
        sql = f"""
            COPY (
                SELECT "Time", "Item", "Value", "source_file", "created_at", "year", "month"
                FROM merged_data
            )
            TO '{output_dir}'
            (FORMAT 'parquet', PARTITION_BY ('year', 'month'), OVERWRITE)
        """
        duckdb.sql(sql)
        print(f"Output written to: {output_dir}")
    except Exception as e:
        print(e)


def merge_with_existing_data(new_data, output_dir: str):
    """
    既存のParquetデータがある場合、既存データと新規データをマージし、
    重複削除（同じ "Time", "Item" で最新の created_at を残す）したArrow Tableを返す。
    既存データがなければ、new_dataをそのまま返す。
    """

    try:
        # 新規データをDuckDB内のテーブルとして登録
        duckdb.register("new_data", new_data)
        # 既存のParquetファイルをチェック（再帰的に検索）
        existing_files = list(Path(output_dir).glob("**/*.parquet"))
        col_order = (
            '"Time", "Item", "Value", "source_file", "created_at", "year", "month"'
        )
        if existing_files:
            merge_query = f"""
                SELECT {col_order} FROM (
                    SELECT {col_order},
                        row_number() OVER (PARTITION BY "Time", "Item" ORDER BY created_at DESC) AS rn
                    FROM (
                        SELECT {col_order} FROM read_parquet('{output_dir}/**/*.parquet')
                        UNION ALL
                        SELECT {col_order} FROM new_data
                    )
                )
                WHERE rn = 1
            """
            merged_data = duckdb.sql(merge_query).to_arrow_table()
            print("既存データと新規データのマージが完了しました。")
            return merged_data
    except Exception as e:
        print(e)
    else:
        print("既存データが見つからなかったため、新規データのみを使用します。")
        return new_data
