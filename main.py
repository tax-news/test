from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from database import db
from news_fetcher import news_fetcher

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Pydantic models for request/response validation
class ArticleResponse(BaseModel):
    id: int
    title: str
    url: str
    publisher: str
    published_date: str
    summary: str
    thumbnail: str
    language: str
    category: str
    created_at: str
    updated_at: str

class ArticleDetailResponse(ArticleResponse):
    full_content: str

class PaginatedResponse(BaseModel):
    articles: List[ArticleResponse]
    total_count: int
    page: int
    limit: int
    total_pages: int

class StatsResponse(BaseModel):
    total_articles: int
    latest_article_date: Optional[str]
    oldest_article_date: Optional[str]
    recent_api_calls_24h: int
    retention_days: int

class SchedulerStatusResponse(BaseModel):
    scheduler_running: bool
    jobs: List[Dict[str, Any]]
    timezone: str

class ManualFetchResponse(BaseModel):
    status: str
    message: str

# Create FastAPI app
app = FastAPI(
    title=os.getenv('API_TITLE', 'News API'),
    description=os.getenv('API_DESCRIPTION', 'Business News API with automated caching'),
    version=os.getenv('API_VERSION', '1.0.0'),
    debug=os.getenv('API_DEBUG', 'False').lower() == 'true'
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration constants
DEFAULT_PAGE_SIZE = int(os.getenv('DEFAULT_PAGE_SIZE', '20'))
MAX_PAGE_SIZE = int(os.getenv('MAX_PAGE_SIZE', '100'))

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize the application"""
    try:
        logger.info("Starting News API application...")
        
        # Start the news fetcher scheduler
        news_fetcher.start_scheduler()
        
        logger.info("News API application started successfully")
        
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        raise

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup during shutdown"""
    try:
        logger.info("Shutting down News API application...")
        
        # Stop the scheduler
        news_fetcher.stop_scheduler()
        
        logger.info("News API application shut down successfully")
        
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

# Root endpoint
@app.get("/", response_model=Dict[str, Any])
async def root():
    """Welcome endpoint with API information"""
    return {
        "message": "Welcome to the News API",
        "version": os.getenv('API_VERSION', '1.0.0'),
        "description": os.getenv('API_DESCRIPTION', 'Business News API with automated caching'),
        "endpoints": {
            "articles": "/articles - Get paginated news articles",
            "article_detail": "/articles/{id} - Get specific article by ID",
            "search": "/articles?search=query - Search articles",
            "stats": "/stats - Get database statistics",
            "health": "/health - Health check",
            "scheduler": "/admin/scheduler - Get scheduler status",
            "manual_fetch": "/admin/fetch - Manually trigger news fetch"
        },
        "documentation": "/docs - Interactive API documentation"
    }

# Health check endpoint
@app.get("/health", response_model=Dict[str, Any])
async def health_check():
    """Health check endpoint"""
    try:
        # Check database connection
        stats = db.get_database_stats()
        
        # Check scheduler status
        scheduler_status = news_fetcher.get_scheduler_status()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": "connected" if stats else "error",
            "scheduler": "running" if scheduler_status.get('scheduler_running', False) else "stopped",
            "total_articles": stats.get('total_articles', 0)
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")

# Get articles with pagination and filtering
@app.get("/articles", response_model=PaginatedResponse)
async def get_articles(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description="Items per page"),
    search: Optional[str] = Query(None, description="Search in title and summary"),
    date_from: Optional[str] = Query(None, description="Filter from date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Filter to date (YYYY-MM-DD)")
):
    """Get paginated news articles with optional filtering"""
    try:
        result = db.get_articles(
            page=page,
            limit=limit,
            search=search,
            date_from=date_from,
            date_to=date_to
        )
        
        return PaginatedResponse(**result)
    
    except Exception as e:
        logger.error(f"Error getting articles: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Get specific article by ID
@app.get("/articles/{article_id}", response_model=ArticleDetailResponse)
async def get_article(
    article_id: int = Path(..., ge=1, description="Article ID")
):
    """Get a specific article by ID"""
    try:
        article = db.get_article_by_id(article_id)
        
        if not article:
            raise HTTPException(status_code=404, detail="Article not found")
        
        return ArticleDetailResponse(**article)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting article {article_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Get database statistics
@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    """Get database and API statistics"""
    try:
        stats = db.get_database_stats()
        
        if not stats:
            raise HTTPException(status_code=500, detail="Unable to retrieve statistics")
        
        return StatsResponse(**stats)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Admin endpoint - Get scheduler status
@app.get("/admin/scheduler", response_model=SchedulerStatusResponse)
async def get_scheduler_status():
    """Get scheduler status and job information (Admin endpoint)"""
    try:
        status = news_fetcher.get_scheduler_status()
        
        if 'error' in status:
            raise HTTPException(status_code=500, detail=status['error'])
        
        return SchedulerStatusResponse(**status)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting scheduler status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Admin endpoint - Manual news fetch
@app.post("/admin/fetch", response_model=ManualFetchResponse)
async def manual_fetch():
    """Manually trigger news fetch (Admin endpoint)"""
    try:
        result = news_fetcher.manual_fetch()
        
        if result['status'] == 'error':
            raise HTTPException(status_code=500, detail=result['message'])
        
        return ManualFetchResponse(**result)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in manual fetch: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Admin endpoint - Manual cleanup
@app.post("/admin/cleanup", response_model=Dict[str, Any])
async def manual_cleanup():
    """Manually trigger database cleanup (Admin endpoint)"""
    try:
        deleted_count = db.cleanup_old_articles()
        
        return {
            "status": "success",
            "message": f"Cleanup completed successfully",
            "deleted_articles": deleted_count
        }
    
    except Exception as e:
        logger.error(f"Error in manual cleanup: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Latest articles endpoint (convenience)
@app.get("/latest", response_model=List[ArticleResponse])
async def get_latest_articles(
    limit: int = Query(10, ge=1, le=50, description="Number of latest articles")
):
    """Get latest articles (convenience endpoint)"""
    try:
        result = db.get_articles(page=1, limit=limit)
        return result['articles']
    
    except Exception as e:
        logger.error(f"Error getting latest articles: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Custom exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "type": "server_error"}
    )

# Run the application
if __name__ == "__main__":
    import uvicorn
    
    host = os.getenv('API_HOST', '0.0.0.0')
    port = int(os.getenv('API_PORT', '8000'))
    debug = os.getenv('API_DEBUG', 'False').lower() == 'true'
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=debug,
        log_level=os.getenv('LOG_LEVEL', 'info').lower()
    )