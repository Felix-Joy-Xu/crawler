import os
import json
import time
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import requests
import pandas as pd
from glob import glob
from dotenv import load_dotenv

# 加载 .env 环境变量文件
load_dotenv()

# ==========================================
# 配置区
# ==========================================
# 从环境变量读取 Token (优先读取单个变量 GITHUB_TOKEN_X，其次读取逗号分隔的 GITHUB_TOKENS)
env_tokens = []
for i in range(1, 20):
    t = os.getenv(f"GITHUB_TOKEN_{i}")
    if t:
        env_tokens.append(t)

if not env_tokens:
    raw_tokens = os.getenv("GITHUB_TOKENS", "")
    env_tokens = [t.strip() for t in raw_tokens.split(",") if t.strip()]

# 最终去重并移除空值
TOKENS = list(dict.fromkeys([t for t in env_tokens if t]))

# 抓取时间范围 (UTC)
START_DATE = datetime.datetime(2022, 1, 1, 0, 0, 0)
END_DATE = datetime.datetime(2026, 7, 1, 0, 0, 0)

# 目标仓库列表 (建议先用 1 个仓库测试)
REPOSITORIES = [
    "ohmyzsh/ohmyzsh",
    "vuejs/vue",
    "twbs/bootstrap",
    "vercel/next.js",
    "mrdoob/three.js",
    "axios/axios",
    "d3/d3",
    "godotengine/godot",
    "rust-lang/rust",
    "ytdl-org/youtube-dl",
    "yt-dlp/yt-dlp",
    "huggingface/transformers",
    "AUTOMATIC1111/stable-diffusion-webui",
    "langchain-ai/langchain",
    "Genymobile/scrcpy",
    "denoland/deno",
    "n8n-io/n8n",
    "excalidraw/excalidraw",
    "rustdesk/rustdesk",
    "fatedier/frp",
    "2dust/v2rayN",
    "massgravel/Microsoft-Activation-Scripts",
    "webpack/webpack"
]

# 数据输出目录 (本地运行时存入D盘指定目录；GitHub Actions 云端运行时存入相对目录)
if os.name == 'nt':
    OUTPUT_DIR = r"D:\国际比较政治经济学\02-原始数据\01-GitHub数据\仓库"
else:
    OUTPUT_DIR = "data_phase1_full"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 并发与限流配置
MAX_WORKERS = 6          # 并发线程数
GLOBAL_RPS = 10          # 全局每秒最大请求数 (防次级限流核心)

# ==========================================
# 核心组件 1: 全局令牌桶限流器 (支持自适应降速)
# ==========================================
class GlobalRateLimiter:
    def __init__(self, initial_rps=10):
        self.capacity = initial_rps
        self.tokens = initial_rps
        self.last_update = time.time()
        self.lock = threading.Lock()
        self.consecutive_failures = 0
        self.max_rps = initial_rps
        self.min_rps = 2  # 最小 RPS，避免限流至零导致彻底停滞
    
    def acquire(self):
        sleep_time = 0
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.capacity)
            self.last_update = now
            if self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.capacity
                self.tokens = 0
            else:
                self.tokens -= 1
        
        if sleep_time > 0:
            time.sleep(sleep_time)

    def on_rate_limit_hit(self):
        """当触发次级限流时，动态折半降速"""
        with self.lock:
            self.consecutive_failures += 1
            if self.consecutive_failures >= 3:
                new_rps = max(self.min_rps, self.capacity // 2)
                if new_rps != self.capacity:
                    print(f"[限流防护] 连续触发限流，将 RPS 从 {self.capacity} 降至 {new_rps}")
                    self.capacity = new_rps
                    self.tokens = new_rps
                self.consecutive_failures = 0

    def on_success(self):
        """成功时逐渐恢复 RPS"""
        with self.lock:
            if self.capacity < self.max_rps:
                self.capacity = min(self.max_rps, self.capacity + 1)
                self.consecutive_failures = 0

# ==========================================
# 核心组件 2: 动态 Token 池管理器 (含隔离机制)
# ==========================================
class TokenManager:
    def __init__(self, tokens):
        # 去重并保留顺序
        self.tokens = list(dict.fromkeys(tokens))
        self.lock = threading.Lock()
        self.token_status = {
            t: {
                'remaining': 5000, 
                'reset_at': time.time() + 3600,
                'error_count': 0,
                'cooldown_until': 0  # 次级限流冷却时间戳
            } 
            for t in self.tokens
        }
    
    def get_best_token(self):
        while True:
            sleep_sec = 0
            with self.lock:
                now = time.time()
                best_token = None
                max_remaining = -1
                
                # 1. 自动重置配额已过期的 Token
                for t, status in self.token_status.items():
                    if now >= status['reset_at']:
                        status['remaining'] = 5000
                        status['reset_at'] = now + 3600
                        status['error_count'] = 0
                
                # 2. 寻找最佳可用 Token
                for t, status in self.token_status.items():
                    # 检查是否在冷却期
                    if now < status['cooldown_until']:
                        continue
                    # 检查错误频次是否超限
                    if status['error_count'] >= 3:
                        continue
                    
                    if status['remaining'] > max_remaining:
                        max_remaining = status['remaining']
                        best_token = t
                
                # 3. 如果找到有配额的可用 Token
                if best_token and max_remaining > 0:
                    self.token_status[best_token]['remaining'] -= 1
                    return best_token
                
                # 4. 如果没有找到可用 Token，计算最短睡眠时间并在锁外休眠
                avail_times = []
                for t, status in self.token_status.items():
                    t_avail = now
                    # 冷却期限制
                    if status['cooldown_until'] > t_avail:
                        t_avail = status['cooldown_until']
                    # 配额耗尽或错误过多的重置限制
                    if status['remaining'] <= 0 or status['error_count'] >= 3:
                        if status['reset_at'] > t_avail:
                            t_avail = status['reset_at']
                    avail_times.append(t_avail)
                
                min_avail = min(avail_times) if avail_times else now + 60
                sleep_sec = max(0, min_avail - now) + 2  # 加 2 秒缓冲区
                print(f"[TokenPool] 所有 Token 冷却中或已耗尽，全局休眠 {sleep_sec:.0f} 秒...")
            
            # 在锁外休眠，释放锁以便其他线程操作
            time.sleep(sleep_sec)

    def update_status(self, token, headers):
        with self.lock:
            if token in self.token_status:
                if 'X-RateLimit-Remaining' in headers:
                    self.token_status[token]['remaining'] = int(headers['X-RateLimit-Remaining'])
                if 'X-RateLimit-Reset' in headers:
                    self.token_status[token]['reset_at'] = float(headers['X-RateLimit-Reset'])

    def mark_token_rate_limited(self, token, retry_after):
        """将 Token 标记为次级限流，强制隔离冷却"""
        with self.lock:
            if token in self.token_status:
                cooldown_time = time.time() + retry_after
                self.token_status[token]['cooldown_until'] = cooldown_time
                self.token_status[token]['error_count'] += 1

# ==========================================
# 核心组件 3: GraphQL 执行器
# ==========================================
class GitHubGraphQLClient:
    def __init__(self, token_manager, rate_limiter):
        self.token_manager = token_manager
        self.rate_limiter = rate_limiter
        self.endpoint = "https://api.github.com/graphql"
        # 使用 threading.local 为每个线程维护独立的 Session
        self._thread_local = threading.local()
    
    def _get_session(self):
        """获取当前线程专属的 requests.Session"""
        if not hasattr(self._thread_local, 'session'):
            self._thread_local.session = requests.Session()
        return self._thread_local.session
    
    def execute(self, query, variables):
        max_retries = 5
        session = self._get_session()
        for attempt in range(max_retries):
            self.rate_limiter.acquire()
            token = self.token_manager.get_best_token()
            headers = {"Authorization": f"bearer {token}"}
            
            try:
                response = session.post(
                    self.endpoint, json={"query": query, "variables": variables}, 
                    headers=headers, timeout=(10, 30)
                )
                
                self.token_manager.update_status(token, response.headers)
                
                if response.status_code in [403, 429]:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    print(f"[限流警告] Token {token[:8]}... 触发次级限流，隔离 {retry_after} 秒。")
                    # 1. 隔离该 Token
                    self.token_manager.mark_token_rate_limited(token, retry_after)
                    # 2. 动态降低限流速率
                    self.rate_limiter.on_rate_limit_hit()
                    # 3. 线程休眠，避免空转
                    time.sleep(retry_after + 2)
                    continue
                
                response.raise_for_status()
                data = response.json()
                if 'errors' in data:
                    is_rate_limit = any(
                        isinstance(err, dict) and (err.get('type') == 'RATE_LIMIT' or err.get('code') == 'graphql_rate_limit')
                        for err in data['errors']
                    )
                    if is_rate_limit:
                        print(f"[限流警告] Token {token[:8]}... 触发 GraphQL 配额耗尽，隔离 15 分钟。")
                        self.token_manager.mark_token_rate_limited(token, 900)
                        self.rate_limiter.on_rate_limit_hit()
                        continue
                    
                    print(f"[GraphQL错误] {data['errors']}")
                    time.sleep(5)
                    continue
                
                # 成功请求时，触发限流速率恢复
                self.rate_limiter.on_success()
                return data['data']
            except Exception as e:
                print(f"[网络错误] {e}，等待 5 秒重试...")
                time.sleep(5)
        return None

# ==========================================
# 辅助函数: 原子化存储与状态管理
# ==========================================
def save_jsonl(file_path, data_list):
    if not data_list: return
    with open(file_path, 'a', encoding='utf-8') as f:
        for item in data_list:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

def load_state(state_file):
    if os.path.exists(state_file):
        with open(state_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_state(state_file, state):
    temp_file = state_file + ".tmp"
    with open(temp_file, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(temp_file, state_file)

# ==========================================
# 抓取逻辑: 阶段一 (广度获取列表)
# ==========================================
def fetch_entity_list(client, owner, name, entity_type, repo_name):
    cursor = None
    numbers = []
    
    # Discussions has no 'state' field in GitHub GraphQL API schema
    state_field = "" if entity_type == "discussions" else "state"
    
    query = """
    query($owner: String!, $name: String!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        %s(first: 100, after: $cursor, orderBy: {field: CREATED_AT, direction: ASC}) {
          pageInfo { endCursor hasNextPage }
          nodes { number title createdAt %s }
        }
      }
    }
    """ % (entity_type, state_field)

    while True:
        data = client.execute(query, {"owner": owner, "name": name, "cursor": cursor})
        if data is None:
            raise RuntimeError(f"拉取 {entity_type} 列表发生网络/API故障，中止以防止状态损坏")
        if not data.get('repository'): break
        
        nodes = data['repository'][entity_type]['nodes']
        stop_loop = False
        
        for node in nodes:
            created_at = datetime.datetime.strptime(node['createdAt'], "%Y-%m-%dT%H:%M:%SZ")
            if created_at < START_DATE: continue
            if created_at > END_DATE:
                stop_loop = True
                break
            numbers.append(node['number'])
            node['repo'] = repo_name
            base_file = os.path.join(OUTPUT_DIR, f"{owner}_{name}_{entity_type}.jsonl")
            save_jsonl(base_file, [node])
        
        if stop_loop or not data['repository'][entity_type]['pageInfo']['hasNextPage']:
            break
        cursor = data['repository'][entity_type]['pageInfo']['endCursor']
        
    return numbers

# ==========================================
# 抓取逻辑: 阶段二 (深度无截断抓取)
# ==========================================
def deep_fetch_pr_optimized(client, owner, name, pr_number, repo_name):
    # 1. 先查询 PR 基础信息
    query_base = """
    query($owner: String!, $name: String!, $number: Int!) {
      repository(owner: $owner, name: $name) {
        pullRequest(number: $number) {
          number title body state createdAt mergedAt
          additions deletions author { login }
        }
      }
    }
    """
    data = client.execute(query_base, {"owner": owner, "name": name, "number": pr_number})
    if data is None:
        raise RuntimeError(f"拉取 PR #{pr_number} 基础信息失败")
    if not data.get('repository') or not data['repository'].get('pullRequest'):
        return
        
    pr_base = data['repository']['pullRequest']
    author_login = pr_base.get('author', {}).get('login', 'ghost') if pr_base.get('author') else 'ghost'
    pr_data = {
        "repo": repo_name,
        "number": pr_base['number'],
        "title": pr_base.get('title', ''),
        "body": pr_base.get('body', '') or "",
        "state": pr_base.get('state', ''),
        "createdAt": pr_base['createdAt'],
        "mergedAt": pr_base.get('mergedAt'),
        "additions": pr_base.get('additions', 0),
        "deletions": pr_base.get('deletions', 0),
        "author": author_login
    }
    save_jsonl(os.path.join(OUTPUT_DIR, f"{owner}_{name}_pr_base.jsonl"), [pr_data])

    # 2. 分页查询普通评论
    cursor = None
    query_comments = """
    query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        pullRequest(number: $number) {
          comments(first: 50, after: $cursor) {
            pageInfo { endCursor hasNextPage }
            nodes { body createdAt author { login } }
          }
        }
      }
    }
    """
    while True:
        data = client.execute(query_comments, {"owner": owner, "name": name, "number": pr_number, "cursor": cursor})
        if data is None:
            raise RuntimeError(f"拉取 PR #{pr_number} 评论失败")
        
        pr_obj = data.get('repository', {}).get('pullRequest')
        if not pr_obj or not pr_obj.get('comments'): break
        
        comments_data = pr_obj['comments']
        comments = comments_data['nodes']
        for c in comments:
            c['author'] = c.get('author', {}).get('login', 'ghost') if c.get('author') else 'ghost'
            c['repo'] = repo_name
            c['pr_number'] = pr_number
        save_jsonl(os.path.join(OUTPUT_DIR, f"{owner}_{name}_pr_comments.jsonl"), comments)
        
        if not comments_data['pageInfo']['hasNextPage']: break
        cursor = comments_data['pageInfo']['endCursor']

    # 3. 分页查询代码审查及嵌套回复
    cursor = None
    query_reviews = """
    query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        pullRequest(number: $number) {
          reviews(first: 50, after: $cursor) {
            pageInfo { endCursor hasNextPage }
            nodes { 
              body state submittedAt author { login }
              comments(first: 50) { nodes { body createdAt author { login } } }
            }
          }
        }
      }
    }
    """
    while True:
        data = client.execute(query_reviews, {"owner": owner, "name": name, "number": pr_number, "cursor": cursor})
        if data is None:
            raise RuntimeError(f"拉取 PR #{pr_number} 代码审查失败")
            
        pr_obj = data.get('repository', {}).get('pullRequest')
        if not pr_obj or not pr_obj.get('reviews'): break
        
        reviews_data = pr_obj['reviews']
        reviews = reviews_data['nodes']
        flatten_reviews = []
        for r in reviews:
            review_author = r.get('author', {}).get('login', 'ghost') if r.get('author') else 'ghost'
            flatten_reviews.append({
                "repo": repo_name, "pr_number": pr_number, "type": "review",
                "body": r.get('body', ''), "state": r.get('state', ''), 
                "createdAt": r.get('submittedAt'), "author": review_author
            })
            for rc in r.get('comments', {}).get('nodes', []):
                rc_author = rc.get('author', {}).get('login', 'ghost') if rc.get('author') else 'ghost'
                flatten_reviews.append({
                    "repo": repo_name, "pr_number": pr_number, "type": "review_comment",
                    "body": rc.get('body', ''), "createdAt": rc.get('createdAt'), "author": rc_author
                })
        save_jsonl(os.path.join(OUTPUT_DIR, f"{owner}_{name}_pr_reviews.jsonl"), flatten_reviews)
        
        if not reviews_data['pageInfo']['hasNextPage']: break
        cursor = reviews_data['pageInfo']['endCursor']

    # 4. 分页查询时间线 (合并、关闭等历程)
    cursor = None
    query_timeline = """
    query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        pullRequest(number: $number) {
          timelineItems(first: 50, after: $cursor) {
            pageInfo { endCursor hasNextPage }
            nodes {
              __typename
              ... on MergedEvent { createdAt actor { login } }
              ... on ClosedEvent { createdAt actor { login } }
              ... on ReopenedEvent { createdAt actor { login } }
            }
          }
        }
      }
    }
    """
    while True:
        data = client.execute(query_timeline, {"owner": owner, "name": name, "number": pr_number, "cursor": cursor})
        if data is None:
            raise RuntimeError(f"拉取 PR #{pr_number} 时间线失败")
            
        pr_obj = data.get('repository', {}).get('pullRequest')
        if not pr_obj or not pr_obj.get('timelineItems'): break
        
        timeline_data = pr_obj['timelineItems']
        timeline = timeline_data['nodes']
        for t in timeline:
            t['repo'] = repo_name
            t['pr_number'] = pr_number
            if 'actor' in t:
                t['actor'] = t.get('actor', {}).get('login', 'ghost') if t.get('actor') else 'ghost'
        save_jsonl(os.path.join(OUTPUT_DIR, f"{owner}_{name}_pr_timeline.jsonl"), timeline)
        
        if not timeline_data['pageInfo']['hasNextPage']: break
        cursor = timeline_data['pageInfo']['endCursor']

def deep_fetch_issue_optimized(client, owner, name, issue_number, repo_name):
    # 1. 拉取 Issue 基础信息
    query_base = """
    query($owner: String!, $name: String!, $number: Int!) {
      repository(owner: $owner, name: $name) {
        issue(number: $number) {
          number title body state createdAt author { login }
        }
      }
    }
    """
    data = client.execute(query_base, {"owner": owner, "name": name, "number": issue_number})
    if data is None:
        raise RuntimeError(f"拉取 Issue #{issue_number} 基础信息失败")
    if not data.get('repository') or not data['repository'].get('issue'):
        return
        
    issue_base = data['repository']['issue']
    author_login = issue_base.get('author', {}).get('login', 'ghost') if issue_base.get('author') else 'ghost'
    issue_data = {
        "repo": repo_name,
        "number": issue_base['number'],
        "title": issue_base.get('title', ''),
        "body": issue_base.get('body', '') or "",
        "state": issue_base.get('state', ''),
        "createdAt": issue_base['createdAt'],
        "author": author_login
    }
    save_jsonl(os.path.join(OUTPUT_DIR, f"{owner}_{name}_issue_base.jsonl"), [issue_data])

    # 2. 分页拉取评论
    cursor = None
    query_comments = """
    query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        issue(number: $number) {
          comments(first: 50, after: $cursor) {
            pageInfo { endCursor hasNextPage }
            nodes { body createdAt author { login } }
          }
        }
      }
    }
    """
    while True:
        data = client.execute(query_comments, {"owner": owner, "name": name, "number": issue_number, "cursor": cursor})
        if data is None:
            raise RuntimeError(f"拉取 Issue #{issue_number} 评论失败")
            
        issue_obj = data.get('repository', {}).get('issue')
        if not issue_obj or not issue_obj.get('comments'): break
        
        comments_data = issue_obj['comments']
        comments = comments_data['nodes']
        for c in comments:
            c['author'] = c.get('author', {}).get('login', 'ghost') if c.get('author') else 'ghost'
            c['repo'] = repo_name
            c['issue_number'] = issue_number
        save_jsonl(os.path.join(OUTPUT_DIR, f"{owner}_{name}_issue_comments.jsonl"), comments)
        
        if not comments_data['pageInfo']['hasNextPage']: break
        cursor = comments_data['pageInfo']['endCursor']

def deep_fetch_discussion_optimized(client, owner, name, disc_number, repo_name):
    # 1. 拉取 Discussion 基础数据
    query_base = """
    query($owner: String!, $name: String!, $number: Int!) {
      repository(owner: $owner, name: $name) {
        discussion(number: $number) {
          number title body createdAt author { login }
        }
      }
    }
    """
    data = client.execute(query_base, {"owner": owner, "name": name, "number": disc_number})
    if data is None:
        raise RuntimeError(f"拉取 Discussion #{disc_number} 基础数据失败")
    if not data.get('repository') or not data['repository'].get('discussion'):
        return
        
    disc_base = data['repository']['discussion']
    author_login = disc_base.get('author', {}).get('login', 'ghost') if disc_base.get('author') else 'ghost'
    disc_data = {
        "repo": repo_name,
        "number": disc_base['number'],
        "title": disc_base.get('title', ''),
        "body": disc_base.get('body', '') or "",
        "createdAt": disc_base['createdAt'],
        "author": author_login
    }
    save_jsonl(os.path.join(OUTPUT_DIR, f"{owner}_{name}_disc_base.jsonl"), [disc_data])

    # 2. 分页拉取评论
    cursor = None
    query_comments = """
    query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        discussion(number: $number) {
          comments(first: 50, after: $cursor) {
            pageInfo { endCursor hasNextPage }
            nodes {
              id body createdAt author { login }
              replies(first: 100) { totalCount nodes { body createdAt author { login } } }
            }
          }
        }
      }
    }
    """
    while True:
        data = client.execute(query_comments, {"owner": owner, "name": name, "number": disc_number, "cursor": cursor})
        if data is None:
            raise RuntimeError(f"拉取 Discussion #{disc_number} 主评论失败")
            
        disc_obj = data.get('repository', {}).get('discussion')
        if not disc_obj or not disc_obj.get('comments'): break
        
        comments_data = disc_obj['comments']
        comments = comments_data['nodes']
        flatten_comments = []
        
        for c in comments:
            main_author = c.get('author', {}).get('login', 'ghost') if c.get('author') else 'ghost'
            flatten_comments.append({
                "repo": repo_name, "disc_number": disc_number,
                "body": c['body'], "createdAt": c['createdAt'], "author": main_author, "type": "comment"
            })
            
            replies_data = c.get('replies', {})
            total_replies = replies_data.get('totalCount', 0)
            for r in replies_data.get('nodes', []):
                reply_author = r.get('author', {}).get('login', 'ghost') if r.get('author') else 'ghost'
                flatten_comments.append({
                    "repo": repo_name, "disc_number": disc_number,
                    "body": r['body'], "createdAt": r['createdAt'], "author": reply_author, "type": "reply"
                })
            
            # 回复超过 100 条时独立分页
            if total_replies > 100:
                reply_cursor = None
                comment_node_id = c['id']
                while True:
                    query_replies = """
                    query($node_id: ID!, $reply_cursor: String) {
                      node(id: $node_id) {
                        ... on DiscussionComment {
                          replies(first: 100, after: $reply_cursor) {
                            pageInfo { endCursor hasNextPage }
                            nodes { body createdAt author { login } }
                          }
                        }
                      }
                    }
                    """
                    reply_data = client.execute(query_replies, {"node_id": comment_node_id, "reply_cursor": reply_cursor})
                    if reply_data is None:
                        raise RuntimeError(f"拉取 Discussion #{disc_number} 嵌套回复失败")
                    if not reply_data.get('node'): break
                    
                    current_replies = reply_data['node']['replies']
                    for r in current_replies['nodes']:
                        reply_author = r.get('author', {}).get('login', 'ghost') if r.get('author') else 'ghost'
                        flatten_comments.append({
                            "repo": repo_name, "disc_number": disc_number,
                            "body": r['body'], "createdAt": r['createdAt'], "author": reply_author, "type": "reply"
                        })
                    if not current_replies['pageInfo']['hasNextPage']: break
                    reply_cursor = current_replies['pageInfo']['endCursor']
                    
        save_jsonl(os.path.join(OUTPUT_DIR, f"{owner}_{name}_disc_comments.jsonl"), flatten_comments)
        
        if not comments_data['pageInfo']['hasNextPage']: break
        cursor = comments_data['pageInfo']['endCursor']

# ==========================================
# 抓取逻辑: 弹性重试与错误隔离封装
# ==========================================
def deep_fetch_pr_resilient(client, owner, name, pr_number, repo_name, max_retries=3):
    for attempt in range(max_retries):
        try:
            deep_fetch_pr_optimized(client, owner, name, pr_number, repo_name)
            return True
        except Exception as e:
            print(f"[{repo_name}] PR #{pr_number} 尝试 {attempt + 1}/{max_retries} 失败: {e}")
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))  # 指数退避
    return False

def deep_fetch_issue_resilient(client, owner, name, issue_number, repo_name, max_retries=3):
    for attempt in range(max_retries):
        try:
            deep_fetch_issue_optimized(client, owner, name, issue_number, repo_name)
            return True
        except Exception as e:
            print(f"[{repo_name}] Issue #{issue_number} 尝试 {attempt + 1}/{max_retries} 失败: {e}")
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
    return False

def deep_fetch_discussion_resilient(client, owner, name, disc_number, repo_name, max_retries=3):
    for attempt in range(max_retries):
        try:
            deep_fetch_discussion_optimized(client, owner, name, disc_number, repo_name)
            return True
        except Exception as e:
            print(f"[{repo_name}] Discussion #{disc_number} 尝试 {attempt + 1}/{max_retries} 失败: {e}")
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
    return False

# ==========================================
# 主控调度逻辑 (含断点续传)
# ==========================================
def process_repository(repo_name, graphql_client):
    owner, name = repo_name.split('/')
    state_file = os.path.join(OUTPUT_DIR, f"{owner}_{name}_state.json")
    state = load_state(state_file)

    print(f"[{repo_name}] === 开始处理 ===")
    
    # 检查 Discussions 是否开启
    if 'has_discussions' not in state:
        repo_check_query = """
        query($owner: String!, $name: String!) { repository(owner: $owner, name: $name) { hasDiscussionsEnabled } }
        """
        repo_data = graphql_client.execute(repo_check_query, {"owner": owner, "name": name})
        state['has_discussions'] = repo_data.get('repository', {}).get('hasDiscussionsEnabled', False) if repo_data else False
        save_state(state_file, state)

    # --- 阶段 1: 广度抓取列表 ---
    if 'pr_numbers' not in state:
        state['pr_numbers'] = fetch_entity_list(graphql_client, owner, name, "pullRequests", repo_name)
        save_state(state_file, state)
    if 'issue_numbers' not in state:
        state['issue_numbers'] = fetch_entity_list(graphql_client, owner, name, "issues", repo_name)
        save_state(state_file, state)
    if state['has_discussions'] and 'disc_numbers' not in state:
        state['disc_numbers'] = fetch_entity_list(graphql_client, owner, name, "discussions", repo_name)
        save_state(state_file, state)

    print(f"[{repo_name}] 列表拉取完成: PR({len(state.get('pr_numbers', []))}), Issue({len(state.get('issue_numbers', []))}), Disc({len(state.get('disc_numbers', []))})")
    
    # --- 阶段 2: 深度抓取详情 ---
    # 初始化索引指针与失败列表
    state.setdefault('pr_idx', 0)
    state.setdefault('issue_idx', 0)
    state.setdefault('disc_idx', 0)
    state.setdefault('failed_pr', [])
    state.setdefault('failed_issue', [])
    state.setdefault('failed_disc', [])

    # PR 深度抓取
    pr_list = state.get('pr_numbers', [])
    while state['pr_idx'] < len(pr_list):
        num = pr_list[state['pr_idx']]
        success = deep_fetch_pr_resilient(graphql_client, owner, name, num, repo_name)
        if not success:
            state['failed_pr'].append(num)
        state['pr_idx'] += 1
        
        # 每10个保存一次状态，或最后完成时保存
        if state['pr_idx'] % 10 == 0 or state['pr_idx'] == len(pr_list):
            save_state(state_file, state)
            print(f"[{repo_name}] PR 进度已保存: {state['pr_idx']}/{len(pr_list)} (失败 {len(state['failed_pr'])})")

    # Issue 深度抓取
    issue_list = state.get('issue_numbers', [])
    while state['issue_idx'] < len(issue_list):
        num = issue_list[state['issue_idx']]
        success = deep_fetch_issue_resilient(graphql_client, owner, name, num, repo_name)
        if not success:
            state['failed_issue'].append(num)
        state['issue_idx'] += 1
        
        if state['issue_idx'] % 10 == 0 or state['issue_idx'] == len(issue_list):
            save_state(state_file, state)
            print(f"[{repo_name}] Issue 进度已保存: {state['issue_idx']}/{len(issue_list)} (失败 {len(state['failed_issue'])})")

    # Discussion 深度抓取
    disc_list = state.get('disc_numbers', [])
    if state['has_discussions']:
        while state['disc_idx'] < len(disc_list):
            num = disc_list[state['disc_idx']]
            success = deep_fetch_discussion_resilient(graphql_client, owner, name, num, repo_name)
            if not success:
                state['failed_disc'].append(num)
            state['disc_idx'] += 1
            
            if state['disc_idx'] % 10 == 0 or state['disc_idx'] == len(disc_list):
                save_state(state_file, state)
                print(f"[{repo_name}] Discussion 进度已保存: {state['disc_idx']}/{len(disc_list)} (失败 {len(state['failed_disc'])})")

    state['is_complete'] = True
    save_state(state_file, state)
    print(f"[{repo_name}] === 全量深度抓取完成 ===")

# ==========================================
# 主程序入口
# ==========================================
def main():
    print("=== GitHub 全量深度爬虫启动 ===")
    print(f"加载 Token 数: {len(TOKENS)}")
    print(f"目标仓库数: {len(REPOSITORIES)}")
    
    rate_limiter = GlobalRateLimiter(initial_rps=GLOBAL_RPS)
    token_manager = TokenManager(tokens=TOKENS)
    graphql_client = GitHubGraphQLClient(token_manager, rate_limiter)
    
    # 多线程并发处理多个仓库 (如果仓库数大于1)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_repo = {executor.submit(process_repository, repo, graphql_client): repo for repo in REPOSITORIES}
        for future in as_completed(future_to_repo):
            repo = future_to_repo[future]
            try:
                future.result()
            except Exception as e:
                print(f"[{repo}] 发生致命异常: {e}")
                
    print("\n=== 所有任务结束 ===")

if __name__ == "__main__":
    main()

