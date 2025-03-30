#!/usr/bin/env python3
import psycopg2
import csv
from datetime import datetime
from tabulate import tabulate
import pytz
import os

# テスト結果を保存する配列
test_results = []

# PostgreSQLの接続設定
DB_CONFIGS = [
    {
        "name": "postgres-utc",
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "postgres",
        "database": "timezone_test",
        "container_timezone": "UTC"
    },
    {
        "name": "postgres-jst",
        "host": "localhost", 
        "port": 5433,
        "user": "postgres",
        "password": "postgres",
        "database": "timezone_test",
        "container_timezone": "Asia/Tokyo"
    },
    {
        "name": "postgres-est",
        "host": "localhost",
        "port": 5434,
        "user": "postgres",
        "password": "postgres",
        "database": "timezone_test",
        "container_timezone": "America/New_York"
    }
]

# テストケース
TEST_CASES = [
    {
        "description": "タイムゾーン指定なし",
        "session_timezone": None,
        "values": [
            {"description": "タイムゾーン情報なし", "ts_str": "2023-01-01 12:00:00", "tstz_str": "2023-01-01 12:00:00"},
            {"description": "UTC指定", "ts_str": "2023-01-01 12:00:00 UTC", "tstz_str": "2023-01-01 12:00:00 UTC"},
            {"description": "JST指定", "ts_str": "2023-01-01 12:00:00 +09:00", "tstz_str": "2023-01-01 12:00:00 +09:00"},
            {"description": "EST指定", "ts_str": "2023-01-01 12:00:00 -05:00", "tstz_str": "2023-01-01 12:00:00 -05:00"}
        ]
    },
    {
        "description": "セッションタイムゾーン: UTC",
        "session_timezone": "UTC",
        "values": [
            {"description": "タイムゾーン情報なし", "ts_str": "2023-01-01 12:00:00", "tstz_str": "2023-01-01 12:00:00"},
            {"description": "UTC指定", "ts_str": "2023-01-01 12:00:00 UTC", "tstz_str": "2023-01-01 12:00:00 UTC"},
            {"description": "JST指定", "ts_str": "2023-01-01 12:00:00 +09:00", "tstz_str": "2023-01-01 12:00:00 +09:00"},
            {"description": "EST指定", "ts_str": "2023-01-01 12:00:00 -05:00", "tstz_str": "2023-01-01 12:00:00 -05:00"}
        ]
    },
    {
        "description": "セッションタイムゾーン: Asia/Tokyo",
        "session_timezone": "Asia/Tokyo",
        "values": [
            {"description": "タイムゾーン情報なし", "ts_str": "2023-01-01 12:00:00", "tstz_str": "2023-01-01 12:00:00"},
            {"description": "UTC指定", "ts_str": "2023-01-01 12:00:00 UTC", "tstz_str": "2023-01-01 12:00:00 UTC"},
            {"description": "JST指定", "ts_str": "2023-01-01 12:00:00 +09:00", "tstz_str": "2023-01-01 12:00:00 +09:00"},
            {"description": "EST指定", "ts_str": "2023-01-01 12:00:00 -05:00", "tstz_str": "2023-01-01 12:00:00 -05:00"}
        ]
    },
    {
        "description": "セッションタイムゾーン: America/New_York",
        "session_timezone": "America/New_York",
        "values": [
            {"description": "タイムゾーン情報なし", "ts_str": "2023-01-01 12:00:00", "tstz_str": "2023-01-01 12:00:00"},
            {"description": "UTC指定", "ts_str": "2023-01-01 12:00:00 UTC", "tstz_str": "2023-01-01 12:00:00 UTC"},
            {"description": "JST指定", "ts_str": "2023-01-01 12:00:00 +09:00", "tstz_str": "2023-01-01 12:00:00 +09:00"},
            {"description": "EST指定", "ts_str": "2023-01-01 12:00:00 -05:00", "tstz_str": "2023-01-01 12:00:00 -05:00"}
        ]
    }
]

def run_tests():
    """すべてのデータベース設定に対してテストを実行"""
    for db_config in DB_CONFIGS:
        print(f"\n\n==== {db_config['name']} ({db_config['container_timezone']}) のテスト実行中 ====")
        
        try:
            conn = psycopg2.connect(
                host=db_config["host"],
                port=db_config["port"],
                user=db_config["user"],
                password=db_config["password"],
                database=db_config["database"]
            )
            
            # 環境設定のチェック
            check_environment(conn, db_config)
            
            # 各テストケースを実行
            for test_case in TEST_CASES:
                run_test_case(conn, db_config, test_case)
                
        except Exception as e:
            print(f"エラー: {e}")
        finally:
            if 'conn' in locals() and conn is not None:
                conn.close()
                
    # 結果をマークダウンファイルに保存
    save_results()

def check_environment(conn, db_config):
    """データベース環境設定の確認"""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM show_timezone_settings()")
        settings = cur.fetchall()
        
        print(f"\n{db_config['name']} の環境設定:")
        for setting in settings:
            print(f"  {setting[0]}: {setting[1]}")
            test_results.append({
                "test_type": "環境設定",
                "db_name": db_config["name"],
                "container_timezone": db_config["container_timezone"],
                "session_timezone": "デフォルト",
                "parameter": setting[0],
                "value": setting[1]
            })

def run_test_case(conn, db_config, test_case):
    """特定のテストケースを実行する"""
    session_timezone = test_case["session_timezone"]
    session_desc = session_timezone if session_timezone else "デフォルト"
    
    print(f"\n---- テストケース: {test_case['description']} ----")
    
    with conn.cursor() as cur:
        # セッションのタイムゾーンを設定（必要な場合）
        if session_timezone:
            cur.execute(f"SET timezone TO '{session_timezone}'")
            conn.commit()
            
            # 更新後のタイムゾーン設定を確認
            cur.execute("SELECT current_setting('timezone')")
            current_tz = cur.fetchone()[0]
            print(f"セッションタイムゾーンを {session_timezone} に設定（実際の値: {current_tz}）")
        
        # テストケースのデータをテスト
        for value in test_case["values"]:
            test_timestamp(conn, cur, db_config, session_desc, value)

def test_timestamp(conn, cur, db_config, session_timezone, value):
    """1つのタイムスタンプ値をテストする"""
    try:
        # テーブルをクリア
        cur.execute("TRUNCATE timezone_test")
        
        # データを挿入
        cur.execute(
            "INSERT INTO timezone_test (description, ts, tstz) VALUES (%s, %s, %s) RETURNING id",
            (value["description"], value["ts_str"], value["tstz_str"])
        )
        inserted_id = cur.fetchone()[0]
        conn.commit()
        
        # データを取得
        cur.execute("""
            SELECT 
                description,
                ts,
                ts::TEXT as ts_text,
                tstz,
                tstz::TEXT as tstz_text,
                tstz AT TIME ZONE 'UTC' as tstz_utc,
                tstz AT TIME ZONE 'Asia/Tokyo' as tstz_jst,
                tstz AT TIME ZONE 'America/New_York' as tstz_est
            FROM timezone_test
            WHERE id = %s
        """, (inserted_id,))
        
        result = cur.fetchone()
        
        # 結果を表示
        print(f"\n[{value['description']}]")
        print(f"  入力値: ts={value['ts_str']}, tstz={value['tstz_str']}")
        print(f"  取得値 (timestamp): {result[1]} ({result[2]})")
        print(f"  取得値 (timestamptz): {result[3]} ({result[4]})")
        print(f"  timestamptz AT TIME ZONE 'UTC': {result[5]}")
        print(f"  timestamptz AT TIME ZONE 'Asia/Tokyo': {result[6]}")
        print(f"  timestamptz AT TIME ZONE 'America/New_York': {result[7]}")
        
        # 結果を記録
        test_results.append({
            "test_type": "タイムスタンプ変換",
            "db_name": db_config["name"],
            "container_timezone": db_config["container_timezone"],
            "session_timezone": session_timezone,
            "input_description": value["description"],
            "input_ts": value["ts_str"],
            "input_tstz": value["tstz_str"],
            "output_ts": str(result[2]),
            "output_tstz": str(result[4]),
            "tstz_at_utc": str(result[5]),
            "tstz_at_jst": str(result[6]),
            "tstz_at_est": str(result[7])
        })
        
    except Exception as e:
        print(f"エラー ({value['description']}): {e}")
        
        # エラーを記録
        test_results.append({
            "test_type": "エラー",
            "db_name": db_config["name"],
            "container_timezone": db_config["container_timezone"],
            "session_timezone": session_timezone,
            "input_description": value["description"],
            "input_ts": value["ts_str"],
            "input_tstz": value["tstz_str"],
            "error": str(e)
        })

def save_results():
    """テスト結果をマークダウンファイルに保存"""
    with open('RESULT.md', 'w', encoding='utf-8') as md_file:
        md_file.write("# PostgreSQLタイムゾーンテスト結果\n\n")
        
        # 環境設定の結果
        md_file.write("## 環境設定\n\n")
        
        for db_config in DB_CONFIGS:
            md_file.write(f"### {db_config['name']} (コンテナTZ: {db_config['container_timezone']})\n\n")
            
            # この設定に関する環境情報を抽出
            env_results = [r for r in test_results if r["test_type"] == "環境設定" and r["db_name"] == db_config["name"]]
            
            if env_results:
                headers = ["パラメータ", "値"]
                rows = [[r["parameter"], r["value"]] for r in env_results]
                md_file.write(tabulate(rows, headers, tablefmt="pipe") + "\n\n")
        
        # タイムスタンプテストの結果
        md_file.write("## タイムスタンプテスト結果\n\n")
        
        for db_config in DB_CONFIGS:
            md_file.write(f"### {db_config['name']} (コンテナTZ: {db_config['container_timezone']})\n\n")
            
            # セッションタイムゾーンごとにグループ化
            session_tzs = sorted(set([r["session_timezone"] for r in test_results 
                                if r["test_type"] == "タイムスタンプ変換" and r["db_name"] == db_config["name"]]))
            
            for session_tz in session_tzs:
                md_file.write(f"#### セッションタイムゾーン: {session_tz}\n\n")
                
                # このデータベースとセッションタイムゾーンに関するテスト結果を抽出
                ts_results = [r for r in test_results 
                           if r["test_type"] == "タイムスタンプ変換" 
                           and r["db_name"] == db_config["name"]
                           and r["session_timezone"] == session_tz]
                
                if ts_results:
                    headers = ["入力値の説明", "入力 timestamp", "入力 timestamptz", 
                              "出力 timestamp", "出力 timestamptz", 
                              "timestamptz at UTC", "timestamptz at JST", "timestamptz at EST"]
                    rows = [[r["input_description"], r["input_ts"], r["input_tstz"], 
                           r["output_ts"], r["output_tstz"], 
                           r["tstz_at_utc"], r["tstz_at_jst"], r["tstz_at_est"]] for r in ts_results]
                    md_file.write(tabulate(rows, headers, tablefmt="pipe") + "\n\n")
                
                # エラーの結果を抽出
                error_results = [r for r in test_results 
                              if r["test_type"] == "エラー" 
                              and r["db_name"] == db_config["name"]
                              and r["session_timezone"] == session_tz]
                
                if error_results:
                    md_file.write("##### エラー\n\n")
                    headers = ["入力値の説明", "入力 timestamp", "入力 timestamptz", "エラー"]
                    rows = [[r["input_description"], r["input_ts"], r["input_tstz"], r["error"]] for r in error_results]
                    md_file.write(tabulate(rows, headers, tablefmt="pipe") + "\n\n")
        
        # 概要と結論
        md_file.write("## 概要と結論\n\n")
        md_file.write("### timestamp型とtimestamptz型の違い\n\n")
        md_file.write("- **timestamp型**: タイムゾーン情報を保持しない。入力時にタイムゾーン情報があっても無視される。\n")
        md_file.write("- **timestamptz型**: タイムゾーン情報をUTCに変換して保存。出力時はセッションのタイムゾーンに変換される。\n\n")
        
        md_file.write("### コンテナタイムゾーンとセッションタイムゾーンの影響\n\n")
        md_file.write("- **コンテナタイムゾーン**: PostgreSQLのシステムタイムゾーンとなる。CURRENT_TIMESTAMPなどの関数に影響する。\n")
        md_file.write("- **セッションタイムゾーン**: timestamptz型の値をクライアントに返す際の表示タイムゾーン。\n\n")
        
        md_file.write("### 実用上の注意点\n\n")
        md_file.write("- タイムゾーンを正確に扱うためには、timestamptz型を使用することを推奨。\n")
        md_file.write("- アプリケーション間でタイムスタンプを受け渡す場合は、タイムゾーン情報を明示的に含める。\n")
        md_file.write("- セッションタイムゾーンはアプリケーションの要件に応じて適切に設定する。\n")
        
    print(f"\nテスト結果を RESULT.md に保存しました。")

if __name__ == "__main__":
    run_tests() 