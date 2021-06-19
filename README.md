# 概要
csvファイルを受け取って(/upload)、ワーカーに渡して(celery)、ダウンロード(/download)する

## 環境構築
### redisのインストール
sudo systemctl start redis-server

### pythonの依存関係
```shell
> pip install -r requirements.txt
```
### workerの起動
herokuにデプロイする場合は不要
ローカルで試す場合は起動しておかないと機械学習のタスクが一向に実行されない
```shell
[app/] > celery -A app.celery worker --loglevel=info
```

## 起動
```shell
> python app.py
```

## Endpoint
### upload
- request
  - file : csv file
- response 202
  - download_name : string

### download/{download_name}
- response 404
  - if task not completed : 404
  - completed: csv file