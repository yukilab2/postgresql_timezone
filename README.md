# PostgreSQLタイムゾーンテスト

このプロジェクトは、PostgreSQLのタイムゾーン動作を確認するためのテスト環境です。

## 目的

PostgreSQLの`timestamp`型と`timestamptz`型の動作を様々な条件下で検証し、以下の点を明らかにします：

1. コンテナのタイムゾーン設定がタイムスタンプ処理にどのように影響するか
2. PostgreSQLセッションのタイムゾーン設定がタイムスタンプ処理にどのように影響するか
3. タイムゾーン情報付き・なしのタイムスタンプがどのように扱われるか

## 環境構築

### 前提条件

- Docker および Docker Compose がインストールされていること
- Python 3.6以上がインストールされていること

### セットアップ手順

1. Python仮想環境を作成し有効化します：

```bash
# 仮想環境の作成
python -m venv venv

# 仮想環境の有効化（macOS/Linux）
source venv/bin/activate

# 仮想環境の有効化（Windows）
# venv\Scripts\activate.bat
```

2. 必要なPythonパッケージをインストールします：

```bash
pip install -r requirements.txt
```

3. Dockerコンテナをビルドして起動します：

```bash
docker compose up --build -d
```

## テストの実行

以下のコマンドでテストを実行します：

```bash
python test_timezones.py
```

テスト結果は`RESULT.md`ファイルに保存されます。

## テスト内容

- 3つの異なるタイムゾーン設定（UTC、JST、EST）でPostgreSQLコンテナを実行
- 各コンテナで4つの異なるセッションタイムゾーン設定（デフォルト、UTC、JST、EST）をテスト
- 各設定で以下のタイムスタンプ入力形式をテスト：
  - タイムゾーン情報なし（例：`2023-01-01 12:00:00`）
  - UTCタイムゾーン指定（例：`2023-01-01 12:00:00 UTC`）
  - JSTタイムゾーン指定（例：`2023-01-01 12:00:00 +09:00`）
  - ESTタイムゾーン指定（例：`2023-01-01 12:00:00 -05:00`）

## 片付け

テストが終了したら、以下のコマンドでDockerコンテナを停止・削除します：

```bash
docker compose down
```

仮想環境を終了するには、以下のコマンドを実行します：

```bash
deactivate
```
