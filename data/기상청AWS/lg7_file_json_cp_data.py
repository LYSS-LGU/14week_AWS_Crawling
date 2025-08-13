# C:/data/lg7_file_json_cp_data.py
import mariadb
import sys
import os
import json

try:
    conn_tar = mariadb.connect(
        user="lguplus7",
        password="발급받은_DB_PASSWORD",
        host="localhost",
        port=3306,
        database="cp_data"
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

tar_cur = conn_tar.cursor()

json_path = 'c:/data/ts_data'
# 하위 폴더를 포함한 모든 json 파일을 찾습니다.
json_file_list = []
for root, _, files in os.walk(json_path):
    for file in files:
        if file.endswith('.json'):
            json_file_list.append(os.path.join(root, file))

for json_file_path in json_file_list:
    print(f'json_file_path = {json_file_path}')

    with open(json_file_path, encoding='UTF8') as json_file:
        json_reader = json_file.read()
        json_dic = json.loads( json_reader )

    document_id = json_dic['info'][0]['document_id']
    print( f"document_id = {document_id}")
    contents_title = json_dic['annotation'][0]['contents_title']
    print( f"contents_title = {contents_title}")
    sentence_id = json_dic['annotation'][0]['contents'][0]['sentence_id']
    print( f"sentence_id = {sentence_id}")
    sentence_title = json_dic['annotation'][0]['contents'][0]['sentence_title']
    print( f"sentence_title = {sentence_title}")
    sentence_text = json_dic['annotation'][0]['contents'][0]['sentence_text']
    print( f"sentence_text = {sentence_text}")

    tar_cur.execute(
        "insert into tb_cp(document_id, contents_title, sentence_id, sentence_title, sentence_text, json_data, create_dt ) values (?,?,?,?,?,?,now())",
        (document_id, contents_title, sentence_id, sentence_title, sentence_text, json.dumps(json_dic ) ))
    conn_tar.commit()
    print('insert into cp_data done')