#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import subprocess
import sys
import math
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# ログ設定: コンソールにタイムスタンプ付きで INFO レベル以上を出力
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s: %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

CONFIG_PATH = 'config.json'
FIELD_CONFIG_PATH = 'field_config.json'
MAX_RESULTS_PER_CALL = 1000


def load_or_create_basic_config():
    """
    基本設定ファイル(config.json)を読み込む。
    存在しない場合はテンプレートを作成して終了する。
    """
    if not os.path.exists(CONFIG_PATH):
        template = {
            "jira_url": "https://your-jira-server.com",
            "username": "your-username",
            "password": "your-password",
            "jql": "project = YOURPROJECT AND status = Open",
            "max_workers": 5
        }
        with open(CONFIG_PATH, 'w', encoding='utf-8', errors='replace') as f:
            json.dump(template, f, indent=4, ensure_ascii=False)
        logging.info(f"基本設定ファイルを作成しました: {CONFIG_PATH}。設定を入力して再実行してください。")
        sys.exit(0)
    with open(CONFIG_PATH, 'r', encoding='utf-8', errors='replace') as f:
        return json.load(f)


def load_or_create_field_config(jira_url, auth):
    """
    フィールド設定ファイル(field_config.json)を読み込む。
    存在しない場合はJIRAサーバーからフィールド一覧を取得し、
    テンプレートを作成して終了する。
    """
    if not os.path.exists(FIELD_CONFIG_PATH):
        logging.info("フィールド設定ファイルが見つかりません。JIRAサーバーからフィールド一覧を取得します。")
        cmd = [
            'curl', '--proxy-ntlm', auth and '-u' or '', auth, '-X', 'GET',
            f'{jira_url}/rest/api/2/field'
        ] if auth else [
            'curl', '--proxy-ntlm', '-X', 'GET',
            f'{jira_url}/rest/api/2/field'
        ]
        # 標準出力は UTF-8、変な文字は置換
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
        if result.returncode != 0:
            logging.error(f"フィールド一覧取得に失敗しました: {result.stderr}")
            sys.exit(1)
        try:
            fields = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            logging.error(f"JSON解析エラー: {e}")
            sys.exit(1)
        field_config = []
        for field in fields:
            field_config.append({
                "id": field.get("id"),
                "name": field.get("name"),
                "include": False,
                "include_history": False
            })
        with open(FIELD_CONFIG_PATH, 'w', encoding='utf-8', errors='replace') as f:
            json.dump(field_config, f, indent=4, ensure_ascii=False)
        logging.info(f"フィールド設定ファイルを作成しました: {FIELD_CONFIG_PATH}。設定を入力して再実行してください。")
        sys.exit(0)
    with open(FIELD_CONFIG_PATH, 'r', encoding='utf-8', errors='replace') as f:
        return json.load(f)


def get_total_issues(jira_url, auth, jql):
    """
    JQL にマッチするチケットの総数を取得する。
    maxResults=0 で total のみを取得する。
    """
    logging.info("総チケット数を取得中...")
    cmd = [
        'curl', '--proxy-ntlm', auth and '-u' or '', auth, '-X', 'GET',
        f'{jira_url}/rest/api/2/search?jql={jql}&startAt=0&maxResults=0'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
    if result.returncode != 0:
        logging.error(f"総チケット数取得に失敗しました: {result.stderr}")
        sys.exit(1)
    try:
        data = json.loads(result.stdout)
        total = data.get('total', 0)
        logging.info(f"総チケット数: {total} 件")
        return total
    except json.JSONDecodeError as e:
        logging.error(f"JSON解析エラー: {e}")
        sys.exit(1)


def download_chunk(jira_url, auth, jql, fields_param, expand_param, start_at):
    """
    指定した startAt からチケットを取得し、JSON ファイルに保存する。
    """
    logging.info(f"チケット取得開始: startAt={start_at}")
    url = f'{jira_url}/rest/api/2/search?jql={jql}&startAt={start_at}&maxResults={MAX_RESULTS_PER_CALL}&fields={fields_param}'
    if expand_param:
        url += f'&expand={expand_param}'
    cmd = [
        'curl', '--proxy-ntlm', auth and '-u' or '', auth, '-X', 'GET', url
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
    filename = f'tickets_{start_at}.json'
    if result.returncode == 0:
        with open(filename, 'w', encoding='utf-8', errors='replace') as f:
            f.write(result.stdout)
        logging.info(f"取得完了: {filename}")
    else:
        logging.error(f"取得失敗: startAt={start_at}。エラー: {result.stderr}")


def main():
    config = load_or_create_basic_config()
    jira_url = config.get('jira_url').rstrip('/')
    username = config.get('username')
    password = config.get('password')
    jql = config.get('jql')
    max_workers = config.get('max_workers', 5)
    auth = f'{username}:{password}' if username and password else ''

    field_config = load_or_create_field_config(jira_url, auth)
    included_fields = [f['id'] for f in field_config if f.get('include')]
    fields_param = ','.join(included_fields) if included_fields else '*all'
    expand_param = 'changelog' if any(f.get('include_history') for f in field_config) else ''

    total = get_total_issues(jira_url, auth, jql)
    if total == 0:
        logging.info("取得対象のチケットがありません。")
        return

    num_chunks = math.ceil(total / MAX_RESULTS_PER_CALL)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(num_chunks):
            start_at = i * MAX_RESULTS_PER_CALL
            futures.append(
                executor.submit(download_chunk, jira_url, auth, jql, fields_param, expand_param, start_at)
            )
        for _ in as_completed(futures):
            pass

if __name__ == '__main__':
    main()
