import sqlite3
import json
import os
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from contextlib import contextmanager
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.db_path = os.getenv('DATABASE_PATH', 'news.db')
        self.timeout = int(os.getenv('DATABASE_TIMEOUT', '30'))
        self.retention_days = int(os.getenv('DATA_RETENTION_DAYS', '3'))
        self.init_database()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path, timeout=self.timeout)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def init_database(self):
        """Initialize database and create tables"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS news_articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    url TEXT UNIQUE NOT NULL,
                    publisher TEXT,
                    published_date TEXT,
                    summary TEXT,
                    thumbnail TEXT,
                    language TEXT,
                    category TEXT DEFAULT 'business',
                    full_content TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS api_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    endpoint TEXT NOT NULL,
                    response_code INTEGER,
                    response_time REAL,
                    articles_fetched INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_created_at ON news_articles(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_published_date ON news_articles(published_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_url ON news_articles(url)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_category ON news_articles(category)')
            
            conn.commit()
    
    def insert_article(self, article_data: Dict[str, Any]) -> bool:
        """Insert a new article into the database"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO news_articles 
                    (title, url, publisher, published_date, summary, thumbnail, language, category, full_content, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    article_data.get('title', ''),
                    article_data.get('url', ''),
                    article_data.get('publisher', ''),
                    article_data.get('published_date', ''),
                    article_data.get('summary', ''),
                    article_data.get('thumbnail', ''),
                    article_data.get('language', os.getenv('NEWS_LANGUAGE', 'en-US')),
                    article_data.get('category', 'business'),
                    article_data.get('full_content', '')
                ))
                conn.commit()
                return True
        except sqlite3.Error as e:
            print(f"Database error inserting article: {e}")
            return False
    
    def bulk_insert_articles(self, articles: List[Dict[str, Any]]) -> int:
        """Insert multiple articles in a single transaction"""
        inserted_count = 0
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                for article in articles:
                    try:
                        cursor.execute('''
                            INSERT OR REPLACE INTO news_articles 
                            (title, url, publisher, published_date, summary, thumbnail, language, category, full_content, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ''', (
                            article.get('title', ''),
                            article.get('url', ''),
                            article.get('publisher', ''),
                            article.get('published_date', ''),
                            article.get('summary', ''),
                            article.get('thumbnail', ''),
                            article.get('language', os.getenv('NEWS_LANGUAGE', 'en-US')),
                            article.get('category', 'business'),
                            article.get('full_content', '')
                        ))
                        inserted_count += 1
                    except sqlite3.Error as e:
                        print(f"Error inserting article {article.get('url', 'unknown')}: {e}")
                        continue
                conn.commit()
        except sqlite3.Error as e:
            print(f"Database error in bulk insert: {e}")
        return inserted_count
    
    def get_articles(self, page: int = 1, limit: int = 20, search: Optional[str] = None, 
                    date_from: Optional[str] = None, date_to: Optional[str] = None) -> Dict[str, Any]:
        """Get articles with pagination and optional filtering"""
        offset = (page - 1) * limit
        
        # Build WHERE clause
        where_conditions = []
        params = []
        
        if search:
            where_conditions.append("(title LIKE ? OR summary LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])
        
        if date_from:
            where_conditions.append("created_at >= ?")
            params.append(date_from)
        
        if date_to:
            where_conditions.append("created_at <= ?")
            params.append(date_to)
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get total count
                cursor.execute(f"SELECT COUNT(*) FROM news_articles WHERE {where_clause}", params)
                total_count = cursor.fetchone()[0]
                
                # Get articles
                cursor.execute(f'''
                    SELECT id, title, url, publisher, published_date, summary, thumbnail, 
                           language, category, created_at, updated_at
                    FROM news_articles 
                    WHERE {where_clause}
                    ORDER BY created_at DESC 
                    LIMIT ? OFFSET ?
                ''', params + [limit, offset])
                
                articles = []
                for row in cursor.fetchall():
                    articles.append({
                        'id': row['id'],
                        'title': row['title'],
                        'url': row['url'],
                        'publisher': row['publisher'],
                        'published_date': row['published_date'],
                        'summary': row['summary'],
                        'thumbnail': row['thumbnail'],
                        'language': row['language'],
                        'category': row['category'],
                        'created_at': row['created_at'],
                        'updated_at': row['updated_at']
                    })
                
                return {
                    'articles': articles,
                    'total_count': total_count,
                    'page': page,
                    'limit': limit,
                    'total_pages': (total_count + limit - 1) // limit
                }
        except sqlite3.Error as e:
            print(f"Database error getting articles: {e}")
            return {'articles': [], 'total_count': 0, 'page': page, 'limit': limit, 'total_pages': 0}
    
    def get_article_by_id(self, article_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific article by ID"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id, title, url, publisher, published_date, summary, thumbnail, 
                           language, category, full_content, created_at, updated_at
                    FROM news_articles 
                    WHERE id = ?
                ''', (article_id,))
                
                row = cursor.fetchone()
                if row:
                    return {
                        'id': row['id'],
                        'title': row['title'],
                        'url': row['url'],
                        'publisher': row['publisher'],
                        'published_date': row['published_date'],
                        'summary': row['summary'],
                        'thumbnail': row['thumbnail'],
                        'language': row['language'],
                        'category': row['category'],
                        'full_content': row['full_content'],
                        'created_at': row['created_at'],
                        'updated_at': row['updated_at']
                    }
                return None
        except sqlite3.Error as e:
            print(f"Database error getting article by ID: {e}")
            return None
    
    def cleanup_old_articles(self) -> int:
        """Remove articles older than retention period"""
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM news_articles WHERE created_at < ?', (cutoff_str,))
                deleted_count = cursor.rowcount
                conn.commit()
                print(f"Cleaned up {deleted_count} articles older than {self.retention_days} days")
                return deleted_count
        except sqlite3.Error as e:
            print(f"Database error during cleanup: {e}")
            return 0
    
    def log_api_call(self, endpoint: str, response_code: int, response_time: float, articles_fetched: int):
        """Log API call statistics"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO api_logs (endpoint, response_code, response_time, articles_fetched)
                    VALUES (?, ?, ?, ?)
                ''', (endpoint, response_code, response_time, articles_fetched))
                conn.commit()
        except sqlite3.Error as e:
            print(f"Database error logging API call: {e}")
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get article count
                cursor.execute('SELECT COUNT(*) FROM news_articles')
                article_count = cursor.fetchone()[0]
                
                # Get latest article date
                cursor.execute('SELECT MAX(created_at) FROM news_articles')
                latest_article = cursor.fetchone()[0]
                
                # Get oldest article date
                cursor.execute('SELECT MIN(created_at) FROM news_articles')
                oldest_article = cursor.fetchone()[0]
                
                # Get recent API calls
                cursor.execute('SELECT COUNT(*) FROM api_logs WHERE created_at > datetime("now", "-1 day")')
                recent_api_calls = cursor.fetchone()[0]
                
                return {
                    'total_articles': article_count,
                    'latest_article_date': latest_article,
                    'oldest_article_date': oldest_article,
                    'recent_api_calls_24h': recent_api_calls,
                    'retention_days': self.retention_days
                }
        except sqlite3.Error as e:
            print(f"Database error getting stats: {e}")
            return {}

# Global database instance
db = DatabaseManager()