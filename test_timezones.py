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
            {"description": "JST指定", "ts_str": "2023-01-01 12:00:00 +09:00", "tstz_str": "2023-01-01 12:00:00 +09:00"}
        ]
    },
    {
        "description": "セッションタイムゾーン: UTC",
        "session_timezone": "UTC",
        "values": [
            {"description": "タイムゾーン情報なし", "ts_str": "2023-01-01 12:00:00", "tstz_str": "2023-01-01 12:00:00"},
            {"description": "UTC指定", "ts_str": "2023-01-01 12:00:00 UTC", "tstz_str": "2023-01-01 12:00:00 UTC"},
            {"description": "JST指定", "ts_str": "2023-01-01 12:00:00 +09:00", "tstz_str": "2023-01-01 12:00:00 +09:00"}
        ]
    },
    {
        "description": "セッションタイムゾーン: Asia/Tokyo",
        "session_timezone": "Asia/Tokyo",
        "values": [
            {"description": "タイムゾーン情報なし", "ts_str": "2023-01-01 12:00:00", "tstz_str": "2023-01-01 12:00:00"},
            {"description": "UTC指定", "ts_str": "2023-01-01 12:00:00 UTC", "tstz_str": "2023-01-01 12:00:00 UTC"},
            {"description": "JST指定", "ts_str": "2023-01-01 12:00:00 +09:00", "tstz_str": "2023-01-01 12:00:00 +09:00"}
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
        
        # 1. セッション関数のテスト
        test_session_functions(conn, cur, db_config, session_desc)
        
        # 2. 文字列リテラルのテスト
        for value in test_case["values"]:
            test_timestamp(conn, cur, db_config, session_desc, value)
            
        # 3. Pythonのdatetimeオブジェクトのテスト
        test_python_datetime(conn, cur, db_config, session_desc)
        
        # 4. now()の結果を異なるカラムに挿入するテスト
        test_now_insertion(conn, cur, db_config, session_desc)

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
                tstz AT TIME ZONE 'Asia/Tokyo' as tstz_jst
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
            "tstz_at_jst": str(result[6])
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

def test_python_datetime(conn, cur, db_config, session_timezone):
    """Pythonのdatetimeオブジェクト（naive/aware）の挙動をテストする"""
    try:
        # テーブルをクリア
        cur.execute("TRUNCATE timezone_test")
        
        # テスト用のdatetimeオブジェクトを作成
        naive_dt = datetime(2023, 1, 1, 12, 0, 0)  # タイムゾーン情報なし（naive）
        utc_dt = pytz.UTC.localize(datetime(2023, 1, 1, 12, 0, 0))  # UTC（aware）
        jst_dt = pytz.timezone('Asia/Tokyo').localize(datetime(2023, 1, 1, 12, 0, 0))  # JST（aware）
        
        # テストケース
        dt_test_cases = [
            {"description": "Python naive datetime", "dt": naive_dt},
            {"description": "Python aware datetime (UTC)", "dt": utc_dt},
            {"description": "Python aware datetime (JST)", "dt": jst_dt}
        ]
        
        for dt_case in dt_test_cases:
            dt = dt_case["dt"]
            description = dt_case["description"]
            
            # データを挿入（psycopg2のプレースホルダ使用）
            cur.execute(
                "INSERT INTO timezone_test (description, ts, tstz) VALUES (%s, %s, %s) RETURNING id",
                (description, dt, dt)
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
                    tstz AT TIME ZONE 'Asia/Tokyo' as tstz_jst
                FROM timezone_test
                WHERE id = %s
            """, (inserted_id,))
            
            result = cur.fetchone()
            
            # 結果を表示
            dt_str = str(dt)
            dt_tz_info = str(dt.tzinfo) if dt.tzinfo else "None"
            print(f"\n[{description}]")
            print(f"  入力値: {dt_str} (tzinfo={dt_tz_info})")
            print(f"  取得値 (timestamp): {result[1]} ({result[2]})")
            print(f"  取得値 (timestamptz): {result[3]} ({result[4]})")
            print(f"  timestamptz AT TIME ZONE 'UTC': {result[5]}")
            print(f"  timestamptz AT TIME ZONE 'Asia/Tokyo': {result[6]}")
            
            # 結果を記録
            test_results.append({
                "test_type": "Pythonデータタイプ変換",
                "db_name": db_config["name"],
                "container_timezone": db_config["container_timezone"],
                "session_timezone": session_timezone,
                "input_description": description,
                "input_dt": dt_str,
                "input_dt_tzinfo": dt_tz_info,
                "output_ts": str(result[2]),
                "output_tstz": str(result[4]),
                "tstz_at_utc": str(result[5]),
                "tstz_at_jst": str(result[6])
            })
    
    except Exception as e:
        print(f"エラー (Python datetime テスト): {e}")
        
        # エラーを記録
        test_results.append({
            "test_type": "エラー",
            "db_name": db_config["name"],
            "container_timezone": db_config["container_timezone"],
            "session_timezone": session_timezone,
            "input_description": "Python datetime テスト",
            "error": str(e)
        })

def test_session_functions(conn, cur, db_config, session_timezone):
    """セッション関数 (now(), CURRENT_TIMESTAMP) の挙動をテスト"""
    try:
        # セッション関数の結果を取得
        cur.execute("""
            SELECT 
                now(),
                now()::TEXT as now_text,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP::TEXT as current_timestamp_text,
                now()::timestamp as now_timestamp,
                now()::timestamp::TEXT as now_timestamp_text,
                CURRENT_TIMESTAMP::timestamp as current_timestamp_timestamp,
                CURRENT_TIMESTAMP::timestamp::TEXT as current_timestamp_timestamp_text
        """)
        
        result = cur.fetchone()
        
        # 結果を表示
        print(f"\n[セッション関数テスト]")
        print(f"  now(): {result[0]} ({result[1]})")
        print(f"  CURRENT_TIMESTAMP: {result[2]} ({result[3]})")
        print(f"  now()::timestamp: {result[4]} ({result[5]})")
        print(f"  CURRENT_TIMESTAMP::timestamp: {result[6]} ({result[7]})")
        
        # 結果を記録
        test_results.append({
            "test_type": "セッション関数",
            "db_name": db_config["name"],
            "container_timezone": db_config["container_timezone"],
            "session_timezone": session_timezone,
            "now": str(result[1]),
            "current_timestamp": str(result[3]),
            "now_timestamp": str(result[5]),
            "current_timestamp_timestamp": str(result[7])
        })
        
    except Exception as e:
        print(f"エラー (セッション関数テスト): {e}")
        
        # エラーを記録
        test_results.append({
            "test_type": "エラー",
            "db_name": db_config["name"],
            "container_timezone": db_config["container_timezone"],
            "session_timezone": session_timezone,
            "input_description": "セッション関数テスト",
            "error": str(e)
        })

def test_now_insertion(conn, cur, db_config, session_timezone):
    """now()の結果を異なるカラムに挿入した際の挙動をテスト"""
    try:
        # テーブルをクリア
        cur.execute("TRUNCATE timezone_test")
        
        # now()の結果をタイムスタンプ型とタイムスタンプw/TZ型の両方に挿入
        cur.execute("""
            INSERT INTO timezone_test (description, ts, tstz) 
            VALUES 
                ('now() to ts', now(), NULL),
                ('now() to tstz', NULL, now())
            RETURNING id, description
        """)
        
        inserted_rows = cur.fetchall()
        conn.commit()
        
        for row in inserted_rows:
            inserted_id = row[0]
            description = row[1]
            
            # データを取得
            cur.execute("""
                SELECT 
                    description,
                    ts,
                    ts::TEXT as ts_text,
                    tstz,
                    tstz::TEXT as tstz_text
                FROM timezone_test
                WHERE id = %s
            """, (inserted_id,))
            
            result = cur.fetchone()
            
            # 結果を表示
            print(f"\n[{description}]")
            if result[1] is not None:  # tsに挿入した場合
                print(f"  取得値 (timestamp): {result[1]} ({result[2]})")
            if result[3] is not None:  # tstzに挿入した場合
                print(f"  取得値 (timestamptz): {result[3]} ({result[4]})")
            
            # 結果を記録
            test_results.append({
                "test_type": "now()挿入テスト",
                "db_name": db_config["name"],
                "container_timezone": db_config["container_timezone"],
                "session_timezone": session_timezone,
                "input_description": description,
                "output_ts": str(result[2]) if result[1] is not None else "NULL",
                "output_tstz": str(result[4]) if result[3] is not None else "NULL"
            })
            
    except Exception as e:
        print(f"エラー (now()挿入テスト): {e}")
        
        # エラーを記録
        test_results.append({
            "test_type": "エラー",
            "db_name": db_config["name"],
            "container_timezone": db_config["container_timezone"],
            "session_timezone": session_timezone,
            "input_description": "now()挿入テスト",
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
            
            # セッションタイムゾーン別の結果
            session_timezones = sorted(set([r["session_timezone"] for r in test_results if r["db_name"] == db_config["name"] and r["test_type"] == "タイムスタンプ変換"]))
            
            for session_tz in session_timezones:
                md_file.write(f"#### セッションタイムゾーン: {session_tz}\n\n")
                
                # この設定・セッションに関するタイムスタンプテストの結果を抽出
                ts_results = [r for r in test_results if r["test_type"] == "タイムスタンプ変換" and r["db_name"] == db_config["name"] and r["session_timezone"] == session_tz]
                
                if ts_results:
                    headers = ["入力値の説明", "入力 timestamp", "入力 timestamptz", 
                              "出力 timestamp", "出力 timestamptz", 
                              "timestamptz at UTC", "timestamptz at JST"]
                    rows = [[r["input_description"], r["input_ts"], r["input_tstz"], 
                           r["output_ts"], r["output_tstz"], 
                           r["tstz_at_utc"], r["tstz_at_jst"]] for r in ts_results]
                    md_file.write(tabulate(rows, headers, tablefmt="pipe") + "\n\n")
        
        # Pythonのdatetimeオブジェクトテスト結果
        md_file.write("## Pythonデータタイプテスト結果\n\n")
        
        for db_config in DB_CONFIGS:
            md_file.write(f"### {db_config['name']} (コンテナTZ: {db_config['container_timezone']})\n\n")
            
            # セッションタイムゾーン別の結果
            session_timezones = sorted(set([r["session_timezone"] for r in test_results if r["db_name"] == db_config["name"] and r["test_type"] == "Pythonデータタイプ変換"]))
            
            for session_tz in session_timezones:
                md_file.write(f"#### セッションタイムゾーン: {session_tz}\n\n")
                
                # この設定・セッションに関するPythonデータタイプテストの結果を抽出
                dt_results = [r for r in test_results if r["test_type"] == "Pythonデータタイプ変換" and r["db_name"] == db_config["name"] and r["session_timezone"] == session_tz]
                
                if dt_results:
                    headers = ["入力値の説明", "入力 datetime", "tzinfo", 
                              "出力 timestamp", "出力 timestamptz", 
                              "timestamptz at UTC", "timestamptz at JST"]
                    rows = [[r["input_description"], r["input_dt"], r["input_dt_tzinfo"], 
                           r["output_ts"], r["output_tstz"], 
                           r["tstz_at_utc"], r["tstz_at_jst"]] for r in dt_results]
                    md_file.write(tabulate(rows, headers, tablefmt="pipe") + "\n\n")
        
        # セッション関数テスト結果
        md_file.write("## セッション関数テスト結果\n\n")
        
        for db_config in DB_CONFIGS:
            md_file.write(f"### {db_config['name']} (コンテナTZ: {db_config['container_timezone']})\n\n")
            
            # セッションタイムゾーン別の結果
            session_timezones = sorted(set([r["session_timezone"] for r in test_results if r["db_name"] == db_config["name"] and r["test_type"] == "セッション関数"]))
            
            for session_tz in session_timezones:
                md_file.write(f"#### セッションタイムゾーン: {session_tz}\n\n")
                
                # この設定・セッションに関するセッション関数テストの結果を抽出
                func_results = [r for r in test_results if r["test_type"] == "セッション関数" and r["db_name"] == db_config["name"] and r["session_timezone"] == session_tz]
                
                if func_results:
                    headers = ["関数", "値", "timestampへの変換結果"]
                    rows = []
                    for r in func_results:
                        rows.append(["now()", r["now"], r["now_timestamp"]])
                        rows.append(["CURRENT_TIMESTAMP", r["current_timestamp"], r["current_timestamp_timestamp"]])
                    md_file.write(tabulate(rows, headers, tablefmt="pipe") + "\n\n")
        
        # now()挿入テスト結果
        md_file.write("## now()挿入テスト結果\n\n")
        
        for db_config in DB_CONFIGS:
            md_file.write(f"### {db_config['name']} (コンテナTZ: {db_config['container_timezone']})\n\n")
            
            # セッションタイムゾーン別の結果
            session_timezones = sorted(set([r["session_timezone"] for r in test_results if r["db_name"] == db_config["name"] and r["test_type"] == "now()挿入テスト"]))
            
            for session_tz in session_timezones:
                md_file.write(f"#### セッションタイムゾーン: {session_tz}\n\n")
                
                # この設定・セッションに関するnow()挿入テストの結果を抽出
                now_results = [r for r in test_results if r["test_type"] == "now()挿入テスト" and r["db_name"] == db_config["name"] and r["session_timezone"] == session_tz]
                
                if now_results:
                    headers = ["テスト内容", "取得値 (timestamp)", "取得値 (timestamptz)"]
                    rows = [[r["input_description"], r["output_ts"], r["output_tstz"]] for r in now_results]
                    md_file.write(tabulate(rows, headers, tablefmt="pipe") + "\n\n")
        
        # エラーがあれば記録
        error_results = [r for r in test_results if r["test_type"] == "エラー"]
        if error_results:
            md_file.write("## エラー\n\n")
            
            headers = ["DB", "コンテナTZ", "セッションTZ", "テスト内容", "エラー"]
            rows = [[r["db_name"], r["container_timezone"], r["session_timezone"], 
                   r.get("input_description", "不明"), r["error"]] for r in error_results]
            md_file.write(tabulate(rows, headers, tablefmt="pipe") + "\n\n")
            
        # 概要と結論
        md_file.write("## 概要と結論\n\n")
        
        md_file.write("### timestamp型とtimestamptz型の違い\n\n")
        md_file.write("- **timestamp型**: タイムゾーン情報を保持しない。入力時にタイムゾーン情報があっても無視される。\n")
        md_file.write("- **timestamptz型**: タイムゾーン情報をUTCに変換して保存。出力時はセッションのタイムゾーンに変換される。\n\n")
        
        md_file.write("### コンテナタイムゾーンとセッションタイムゾーンの影響\n\n")
        md_file.write("- **コンテナタイムゾーン**: PostgreSQLのシステムタイムゾーンとなる。CURRENT_TIMESTAMPなどの関数に影響する。\n")
        md_file.write("- **セッションタイムゾーン**: timestamptz型の値をクライアントに返す際の表示タイムゾーン。\n\n")
        
        md_file.write("### Python datetime オブジェクトの挙動\n\n")
        md_file.write("- **naive datetime**: タイムゾーン情報を持たないDatetimeオブジェクトは、timestamptz型に挿入する際にセッションタイムゾーンで解釈される。\n")
        md_file.write("- **aware datetime**: タイムゾーン情報を持つDatetimeオブジェクトは、そのタイムゾーン情報に基づいてUTCに変換されてtimestamptz型に格納される。\n")
        md_file.write("- **timestamp型への挿入**: どちらのdatetimeオブジェクトも、timestamp型に挿入する場合はタイムゾーン情報が無視され、時刻部分のみが格納される。\n\n")
        
        md_file.write("### セッション関数 (now(), CURRENT_TIMESTAMP) の挙動\n\n")
        md_file.write("- これらの関数は常にtimestamptz型で現在時刻を返す。\n")
        md_file.write("- 戻り値はコンテナのシステムタイムゾーンに依存するが、表示はセッションタイムゾーンによって変わる。\n")
        md_file.write("- これらの関数結果をtimestamp型にキャスト（::timestamp）すると、タイムゾーン情報が失われる。\n\n")
        
        md_file.write("### now()の結果をカラムに挿入する場合の挙動\n\n")
        md_file.write("- **timestamp型に挿入**: now()の結果をtimestamp型カラムに挿入すると、タイムゾーン情報が失われ、セッションタイムゾーンでの時刻表現だけが格納される。\n")
        md_file.write("- **timestamptz型に挿入**: now()の結果をそのままtimestamptz型カラムに挿入すると、タイムゾーン情報が保持され、読み出し時にセッションタイムゾーンに応じた表示となる。\n\n")
        
        md_file.write("### 実用上の注意点\n\n")
        md_file.write("- タイムゾーンを正確に扱うためには、timestamptz型を使用することを推奨。\n")
        md_file.write("- アプリケーション間でタイムスタンプを受け渡す場合は、タイムゾーン情報を明示的に含める。\n")
        md_file.write("- セッションタイムゾーンはアプリケーションの要件に応じて適切に設定する。\n")
        md_file.write("- Pythonのnaive datetimeオブジェクトをtimestamptz型に挿入する際は注意が必要。可能な限りawareなdatetimeを使用するか、セッションタイムゾーンを明示的に設定する。\n")
        md_file.write("- now()やCURRENT_TIMESTAMPの結果をカラムに格納する場合、timestamp型とtimestamptz型の違いを理解し、適切な型を選択する。\n")
            
    print(f"\nテスト結果を RESULT.md に保存しました")

if __name__ == "__main__":
    run_tests() 