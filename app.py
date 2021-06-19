from flask import *
import os
from os.path import join, dirname
import csv
import pandas as pd
from time import sleep
import hashlib
from celery import Celery
import numpy as np
import settings
import zlib, json, base64
import pickle
from pymongo import MongoClient
ZIPJSON_KEY='base64(zip(o))'

def json_zip(j):
  j = {
    ZIPJSON_KEY: base64.b64encode(
      zlib.compress(json.dumps(j).encode('utf-8'))
    ).decode('ascii')
  }
  return j

def json_unzip(j, insist=True):
  try:
    assert (j[ZIPJSON_KEY])
    assert (set(j.keys()) == {ZIPJSON_KEY})
  except:
    if insist:
      raise RuntimeError("JSON not in the expected format {" + str(ZIPJSON_KEY) + ":zipstring}")
    else:
      return j
  try:
    j = zlib.decompress(base64.b64decode(j[ZIPJSON_KEY]))
  except:
    raise RuntimeError("Could not decode/unzip th3e contents")
  try:
    j = json.loads(j)
  except:
    raise RuntimeError("Could interpret the unzipped contents")
  return j

# def connect_mdb():
#   client = MongoClient(settings.MONGODB_URI, retryWrites=False)
#   db = client.get_default_database()
#   return {"client": client, "db": db}

def make_celery(app):
  celery = Celery(
    app.name,
    backend=app.config['CELERY_RESULT_BACKEND'],
    broker=app.config['CELERY_BROKER_URL']
  )
  celery.conf.update(app.config)

  class ContextTask(celery.Task):
    def __call__(self, *args, **kwargs):
      with app.app_context():
        return self.run(*args, **kwargs)

  celery.Task = ContextTask
  return celery

app = Flask(__name__)

app.config.update(
  CELERY_BROKER_URL=settings.REDISGREEN_URL,
  CELERY_RESULT_BACKEND=settings.REDISGREEN_URL
)
celery = make_celery(app)

@celery.task()
def predict(filename, test):
  test = pd.read_json(json_unzip(test))

  test['給与/交通費　備考'].fillna('No', inplace=True)
  test['月収'] = test['給与/交通費　備考'].apply(lambda x : x.split('円')[0])
  test['月収'] = test['月収'].apply(lambda x : x.replace('【月収例】', '') if x.find("月収例") >= 0 else None)
  test['月収'].dropna(inplace=True)

  ten_thousand = test['月収'].apply(lambda x : x.split('万')[0]).astype(np.int64)
  number = test['月収'].apply(lambda x : x.split('万')[1] if x.split('万')[1] != "" else 0).astype(np.int64)
  test['月収'] = (ten_thousand * 10000) + number

  del test['給与/交通費　備考']

  nan_columns = ["勤務地　最寄駅3（駅名）", "勤務地固定", "応募先　名称", "勤務地　最寄駅3（沿線名）", "（派遣先）勤務先写真コメント", "勤務地　最寄駅3（分）", "無期雇用派遣", "未使用.14", "（派遣以外）応募後の流れ", "（派遣先）概要　従業員数", "電話応対なし", "週払い", "週1日からOK", "固定残業制 残業代 下限", "ミドル（40〜）活躍中", "ルーティンワークがメイン", "未使用.11", "フリー項目　内容", "先輩からのメッセージ", "対象者設定　年齢下限", "未使用.10", "動画コメント", "未使用.8", "経験必須", "固定残業制 残業代に充当する労働時間数 下限", "給与/交通費　給与支払区分", "ブロックコード2", "未使用.4", "CAD関連のスキルを活かす", "未使用.7", "メモ", "ブロックコード3", "固定残業制", "WEB面接OK", "公開区分", "17時以降出社OK", "寮・社宅あり", "20代活躍中", "検索対象エリア", "就業形態区分", "ネットワーク関連のスキルを活かす", "Wワーク・副業可能", "固定残業制 残業代に充当する労働時間数 上限", "プログラム関連のスキルを活かす", "未使用.15", "30代活躍中", "未使用.12", "エルダー（50〜）活躍中", "（派遣）応募後の流れ", "人材紹介", "主婦(ママ)・主夫歓迎", "雇用形態", "Dip JobsリスティングS", "ブロックコード1", "フリー項目　タイトル", "資格取得支援制度あり", "未使用.1", "ブランクOK", "対象者設定　年齢上限", "未使用.20", "社会保険制度あり", "募集形態", "勤務地　最寄駅3（駅からの交通手段）", "応募先　最寄駅（沿線名）", "仕事写真（下）　写真1　ファイル名", "未使用.16", "仕事写真（下）　写真3　ファイル名", "オープニングスタッフ", "応募先　所在地　ブロックコード", "応募先　所在地　都道府県", "動画タイトル", "（派遣先）概要　事業内容", "応募先　最寄駅（駅名）", "残業月10時間未満", "外国人活躍中・留学生歓迎", "履歴書不要", "未使用.17", "未使用.9", "研修制度あり", "日払い", "未使用", "未使用.18", "未使用.22", "未使用.5", "勤務地　周辺情報", "仕事写真（下）　写真2　ファイル名", "バイク・自転車通勤OK", "仕事写真（下）　写真2　コメント", "DTP関連のスキルを活かす", "未使用.3", "未使用.2", "WEB関連のスキルを活かす", "未使用.6", "給与　経験者給与下限", "学生歓迎", "固定残業制 残業代 上限", "未使用.19", "給与　経験者給与上限", "未使用.21", "待遇・福利厚生", "シニア（60〜）歓迎", "ベンチャー企業", "少人数の職場", "仕事写真（下）　写真3　コメント", "新卒・第二新卒歓迎", "産休育休取得事例あり", "動画ファイル名", "対象者設定　性別", "WEB登録OK", "応募先　備考", "応募先　所在地　市区町村", "仕事写真（下）　写真1　コメント", "応募拠点", "これまでの採用者例", "（派遣先）概要　勤務先名（フリガナ）", "（派遣先）配属先部署　男女比　女", "未使用.13"]

  for nan_column in nan_columns:
      del test[nan_column]

  start_time = test['期間・時間　勤務時間'].apply(lambda x : x.split("〜")[0])
  end_time = test['期間・時間　勤務時間'].apply(lambda x : x.split("〜")[1].split("\u3000")[0])

  start_hour = start_time.apply(lambda x : x.split(':')[0]).astype(np.int)
  start_min = start_time.apply(lambda x : 0.5 if x.split(':')[1] == "30" else 0)
  start_time = start_hour + start_min

  end_hour = end_time.apply(lambda x : x.split(':')[0]).astype(np.int)
  end_min = end_time.apply(lambda x : 0.5 if x.split(':')[1] == "30" else 0)
  end_time = end_hour + end_min

  test['開始時間'] = start_time
  test['終了時間'] = end_time

  test['勤務時間'] = end_time - start_time
  test = test[test['勤務時間'] != 0.]

  new_columns = ['大卒', '新卒', '実務経験']

  for new_column in new_columns:
      test[new_column + "歓迎"] = test['応募資格'].apply(lambda x : 1 if new_column in x else 0)

  test['事務経験歓迎'] = test['応募資格'].apply(lambda x : 1 if (("事務経験" in x) or ("事務の経験" in x)) else 0)
  test["コンビニあり"] = test['お仕事のポイント（仕事PR）'].apply(lambda x : 1 if ("コンビニ" in x) or ('飲食店' in x) else 0)

  test['（紹介予定）待遇・福利厚生'].fillna('NA', inplace=True)
  test['（紹介予定）待遇・福利厚生'] = test['（紹介予定）待遇・福利厚生'].apply(lambda x : 0 if x == 'NA' else 1)

  delete_columns = ['（派遣先）職場の雰囲気', '期間･時間　備考', '（紹介予定）雇用形態備考', '応募資格', 'お仕事のポイント（仕事PR）', '残業月20時間以上', 'お仕事名', '（派遣先）勤務先写真ファイル名', '仕事内容', '（紹介予定）年収・給与例', '（紹介予定）入社後の雇用形態', '休日休暇　備考', '期間・時間　勤務時間', '土日祝のみ勤務', '掲載期間　開始日', '拠点番号', '扶養控除内', '派遣会社のうれしい特典']
  for delete_column in delete_columns:
    del test[delete_column]

  mean_columns = ['（派遣先）配属先部署　人数', '勤務地　最寄駅1（分）', '（派遣先）配属先部署　平均年齢', '（派遣先）配属先部署　男女比　男']
  for mean_column in mean_columns:
    test[mean_column].fillna(test[mean_column].mean(), inplace=True)

  from sklearn.preprocessing import LabelEncoder
  encode_columns = ['掲載期間　終了日', '勤務地　最寄駅2（駅名）', '（派遣先）配属先部署', '勤務地　最寄駅2（沿線名）', '（派遣先）概要　勤務先名（漢字）', '勤務地　備考', '（紹介予定）入社時期', '期間・時間　勤務開始日', '（紹介予定）休日休暇', '勤務地　最寄駅1（駅名）', '勤務地　最寄駅1（沿線名）']

  for c in encode_columns:
    test[c] = test[c].fillna('NA')
    le = LabelEncoder()
    le.fit(test[c])
    test[c] = le.transform(test[c].fillna('NA'))

  import lightgbm as lgb
  with open(join(dirname(__file__), 'input/lightGBM_10000_part1.pickle'), mode='rb') as fp:
    model = pickle.load(fp)

  submit_df = pd.DataFrame({'お仕事No.': test['お仕事No.']})
  X_test = test.drop(['お仕事No.'], axis=1)
  test_pred = model.predict(X_test)
  submit_df["応募数 合計"] = test_pred

  submit_dict = json.dumps(submit_df.to_dict("records"))
  saveData = {'_id': filename, 'df': submit_dict}

  client = MongoClient(settings.MONGODB_URI, retryWrites=False)
  db = client.get_default_database()

  db["results"].insert_one(saveData)

  return render_template('index.html')

@app.route('/', methods=['GET'])
def index():
  return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
  file = request.files['file']

  if (file.content_type == 'text/csv') or (file.content_type == 'application/vnd.ms-excel'):
    df = pd.read_csv(file)
  else:
    return {'status': 'Bad Request'}, 400

  file_hash = hashlib.md5(pd.util.hash_pandas_object(df, index=True).values).hexdigest()
  predict.delay(file_hash, json_zip(df.to_json()))

  return {"file_name": "{}".format(file_hash)}, 200

@app.route('/download/<string:file_name>', methods=['GET'])
def download(file_name):
  client = MongoClient(settings.MONGODB_URI, retryWrites=False)
  db = client.get_default_database()
  cursor = db["results"].find({"_id": file_name})

  if cursor.count() > 0:
    data = db["results"].find_one({"_id": file_name}).get('df')
    dfData = json.loads(data)
    df = pd.DataFrame.from_dict(dfData)
    df.to_csv("tmp/{}.csv".format(file_name))

    downloadFileName = join(dirname(__file__), 'tmp/{}.csv'.format(file_name))

    return send_file(downloadFileName, \
      as_attachment = True, attachment_filename = "{}.csv".format(file_name), \
      mimetype = "text/csv")
  else:
    return {'status': 'Not Found'}, 404

@app.route('/ready/<string:file_name>', methods=['GET'])
def ready(file_name):
  client = MongoClient(settings.MONGODB_URI, retryWrites=False)
  db = client.get_default_database()
  cursor = db["results"].find({"_id": file_name})

  if cursor.count() > 0:
    return {'status': 'ok'}, 200
  else:
    return {'status': 'Not Found'}, 404

@app.route('/favicon.ico', methods=['GET'])
def favicon():
  # return send_from_directory(os.path.join(app.root_path, 'static'),'favicon.ico', mimetype='image/vnd.microsoft.icon')
  return ""
  
if __name__ == '__main__':
  app.run(debug=True)