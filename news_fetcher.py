import http.client
import json
import os
import time
import logging
import datetime
from datetime import datetime
from typing import Dict, List, Any, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from database import db

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.getenv('LOG_FILE', 'news_api.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NewsAPIError(Exception):
    """Custom exception for news API errors"""
    pass

class NewsFetcher:
    def __init__(self):
        self.rapidapi_key = os.getenv('RAPIDAPI_KEY')
        self.rapidapi_host = os.getenv('RAPIDAPI_HOST', 'google-news13.p.rapidapi.com')
        self.news_endpoint = os.getenv('NEWS_ENDPOINT', '/business')
        self.news_language = os.getenv('NEWS_LANGUAGE', 'en-US')
        self.connection_timeout = int(os.getenv('CONNECTION_TIMEOUT', '30'))
        self.read_timeout = int(os.getenv('READ_TIMEOUT', '30'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.include_subnews = os.getenv('INCLUDE_SUBNEWS', 'True').lower() == 'true'
        self.max_articles_per_fetch = int(os.getenv('MAX_ARTICLES_PER_FETCH', '1000'))
        
        if not self.rapidapi_key:
            raise ValueError("RAPIDAPI_KEY is required in environment variables")
        
        self.scheduler = BackgroundScheduler()
        self.setup_scheduler()
    
    def setup_scheduler(self):
        """Setup background scheduler for fetching news and cleanup"""
        try:
            # Schedule news fetching
            fetch_interval = int(os.getenv('FETCH_INTERVAL_HOURS', '2'))
            self.scheduler.add_job(
                func=self.fetch_and_store_news,
                trigger=IntervalTrigger(hours=fetch_interval),
                id='fetch_news',
                name='Fetch News from API',
                replace_existing=True
            )
            
            # Schedule daily cleanup
            cleanup_hour = int(os.getenv('CLEANUP_HOUR', '0'))
            cleanup_minute = int(os.getenv('CLEANUP_MINUTE', '0'))
            self.scheduler.add_job(
                func=self.cleanup_old_data,
                trigger=CronTrigger(hour=cleanup_hour, minute=cleanup_minute),
                id='cleanup_old_data',
                name='Cleanup Old Articles',
                replace_existing=True
            )
            
            logger.info(f"Scheduler configured: Fetch every {fetch_interval}h, Cleanup daily at {cleanup_hour:02d}:{cleanup_minute:02d}")
            
        except Exception as e:
            logger.error(f"Error setting up scheduler: {e}")
            raise
    
    def start_scheduler(self):
        """Start the background scheduler"""
        try:
            if not self.scheduler.running:
                self.scheduler.start()
                logger.info("News fetcher scheduler started successfully")
                
                # Run initial fetch
                self.fetch_and_store_news()
            else:
                logger.info("Scheduler is already running")
        except Exception as e:
            logger.error(f"Error starting scheduler: {e}")
            raise
    
    def stop_scheduler(self):
        """Stop the background scheduler"""
        try:
            if self.scheduler.running:
                self.scheduler.shutdown()
                logger.info("News fetcher scheduler stopped")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")
    
    def fetch_news_from_api(self) -> Optional[Dict[str, Any]]:
        """Fetch news from external API with retry logic"""
        headers = {
            'x-rapidapi-key': self.rapidapi_key,
            'x-rapidapi-host': self.rapidapi_host
        }
        
        endpoint_url = f"{self.news_endpoint}?lr={self.news_language}"
        
        for attempt in range(self.max_retries):
            start_time = time.time()
            conn = None
            
            try:
                logger.info(f"Fetching news from API (attempt {attempt + 1}/{self.max_retries})")
                
                conn = http.client.HTTPSConnection(self.rapidapi_host, timeout=self.connection_timeout)
                conn.request("GET", endpoint_url, headers=headers)
                
                response = conn.getresponse()
                response_time = time.time() - start_time
                
                if response.status == 200:
                    data = response.read()
                    response_data = json.loads(data.decode("utf-8"))
                    
                    logger.info(f"Successfully fetched news data in {response_time:.2f}s")
                    
                    # Log successful API call
                    articles_count = len(response_data.get('articles', [])) if isinstance(response_data, dict) else 0
                    db.log_api_call(endpoint_url, response.status, response_time, articles_count)
                    
                    return response_data
                
                elif response.status == 429:
                    logger.warning(f"Rate limit exceeded (429). Attempt {attempt + 1}")
                    if attempt < self.max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff
                        logger.info(f"Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                    continue
                
                else:
                    error_msg = f"API request failed with status {response.status}"
                    logger.error(error_msg)
                    
                    # Log failed API call
                    db.log_api_call(endpoint_url, response.status, response_time, 0)
                    
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        raise NewsAPIError(error_msg)
            
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    raise NewsAPIError(f"Invalid JSON response: {e}")
            
            except Exception as e:
                logger.error(f"Connection error: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    raise NewsAPIError(f"Connection failed after {self.max_retries} attempts: {e}")
            
            finally:
                if conn:
                    conn.close()
        
        return None
    
    def parse_news_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse and normalize news data from API response"""
        articles = []
        
        try:
            # Check if response is successful
            if not isinstance(raw_data, dict):
                logger.warning("Unexpected response format")
                return articles
            
            # Check status
            if raw_data.get('status') != 'success':
                logger.warning(f"API returned non-success status: {raw_data.get('status')}")
                return articles
            
            # Get items from the response
            news_items = raw_data.get('items', [])
            if not news_items:
                logger.warning("No items found in API response")
                return articles
            
            for item in news_items:
                try:
                    # Convert timestamp to readable date format
                    timestamp = item.get('timestamp', '')
                    published_date = ''
                    if timestamp:
                        try:
                            # Convert timestamp (milliseconds) to datetime
                            import datetime
                            dt = datetime.datetime.fromtimestamp(int(timestamp) / 1000)
                            published_date = dt.strftime('%Y-%m-%d %H:%M:%S')
                        except (ValueError, TypeError):
                            published_date = str(timestamp)
                    
                    # Extract thumbnail from images object
                    thumbnail = ''
                    images = item.get('images', {})
                    if isinstance(images, dict):
                        # Try thumbnailProxied first, then thumbnail
                        thumbnail = images.get('thumbnailProxied', images.get('thumbnail', ''))
                    
                    # Normalize article data according to the actual API structure
                    article = {
                        'title': item.get('title', '').strip(),
                        'url': item.get('newsUrl', '').strip(),
                        'publisher': item.get('publisher', '').strip(),
                        'published_date': published_date,
                        'summary': item.get('snippet', '').strip(),
                        'thumbnail': thumbnail.strip(),
                        'language': self.news_language,
                        'category': 'business',
                        'full_content': ''  # This API doesn't provide full content
                    }
                    
                    # Skip articles with missing essential fields
                    if not article['title'] or not article['url']:
                        logger.warning(f"Skipping article with missing title or URL")
                        continue
                    
                    articles.append(article)
                    
                    # Also process subnews if available
                    subnews = item.get('subnews', [])
                    if isinstance(subnews, list) and item.get('hasSubnews', False):
                        for subitem in subnews:
                            try:
                                # Convert subnews timestamp
                                sub_timestamp = subitem.get('timestamp', '')
                                sub_published_date = ''
                                if sub_timestamp:
                                    try:
                                        dt = datetime.datetime.fromtimestamp(int(sub_timestamp) / 1000)
                                        sub_published_date = dt.strftime('%Y-%m-%d %H:%M:%S')
                                    except (ValueError, TypeError):
                                        sub_published_date = str(sub_timestamp)
                                
                                # Extract subnews thumbnail
                                sub_thumbnail = ''
                                sub_images = subitem.get('images', {})
                                if isinstance(sub_images, dict):
                                    sub_thumbnail = sub_images.get('thumbnailProxied', sub_images.get('thumbnail', ''))
                                
                                sub_article = {
                                    'title': subitem.get('title', '').strip(),
                                    'url': subitem.get('newsUrl', '').strip(),
                                    'publisher': subitem.get('publisher', '').strip(),
                                    'published_date': sub_published_date,
                                    'summary': subitem.get('snippet', '').strip(),
                                    'thumbnail': sub_thumbnail.strip(),
                                    'language': self.news_language,
                                    'category': 'business',
                                    'full_content': ''
                                }
                                
                                if sub_article['title'] and sub_article['url']:
                                    articles.append(sub_article)
                                    
                            except Exception as e:
                                logger.warning(f"Error parsing subnews item: {e}")
                                continue
                    
                except Exception as e:
                    logger.warning(f"Error parsing article: {e}")
                    continue
            
            logger.info(f"Successfully parsed {len(articles)} articles")
            return articles
            
        except Exception as e:
            logger.error(f"Error parsing news data: {e}")
            return []
    
    def fetch_and_store_news(self):
        """Main function to fetch news and store in database"""
        try:
            logger.info("Starting news fetch and store process")
            
            # Fetch news from API
            raw_data = self.fetch_news_from_api()
            if not raw_data:
                logger.warning("No data received from API")
                return
            
            # Parse the data
            articles = self.parse_news_data(raw_data)
            if not articles:
                logger.warning("No articles parsed from API response")
                return
            
            # Limit the number of articles if configured
            if len(articles) > self.max_articles_per_fetch:
                logger.info(f"Limiting articles from {len(articles)} to {self.max_articles_per_fetch}")
                articles = articles[:self.max_articles_per_fetch]
            
            # Store articles in database
            inserted_count = db.bulk_insert_articles(articles)
            
            logger.info(f"Successfully stored {inserted_count} articles in database")
            
            # Log summary
            stats = db.get_database_stats()
            logger.info(f"Database stats: {stats}")
            
        except NewsAPIError as e:
            logger.error(f"News API error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in fetch_and_store_news: {e}")
    
    def cleanup_old_data(self):
        """Clean up old articles from database"""
        try:
            logger.info("Starting database cleanup process")
            deleted_count = db.cleanup_old_articles()
            logger.info(f"Cleanup completed: {deleted_count} old articles removed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """Get scheduler status and job information"""
        try:
            jobs = []
            for job in self.scheduler.get_jobs():
                jobs.append({
                    'id': job.id,
                    'name': job.name,
                    'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
                    'trigger': str(job.trigger)
                })
            
            return {
                'scheduler_running': self.scheduler.running,
                'jobs': jobs,
                'timezone': str(self.scheduler.timezone)
            }
        except Exception as e:
            logger.error(f"Error getting scheduler status: {e}")
            return {'error': str(e)}
    
    def manual_fetch(self) -> Dict[str, Any]:
        """Manually trigger news fetch (for testing/admin purposes)"""
        try:
            logger.info("Manual news fetch triggered")
            self.fetch_and_store_news()
            return {'status': 'success', 'message': 'News fetch completed successfully'}
        except Exception as e:
            logger.error(f"Manual fetch failed: {e}")
            return {'status': 'error', 'message': str(e)}

# Global news fetcher instance
news_fetcher = NewsFetcher()