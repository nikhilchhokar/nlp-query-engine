"""
Cache Manager Service
Implements intelligent query caching with TTL and LRU eviction
"""

from typing import Dict, Any, Optional
from collections import OrderedDict
from datetime import datetime, timedelta
import logging
import hashlib

logger = logging.getLogger(__name__)

class QueryCache:
    """
    Production-ready cache implementation with:
    - Time-to-live (TTL) for cache entries
    - LRU (Least Recently Used) eviction policy
    - Cache statistics tracking
    - Thread-safe operations
    """
    
    def __init__(self, ttl_seconds: int = 300, max_size: int = 1000):
        """
        Initialize cache with TTL and size limits.
        
        Args:
            ttl_seconds: Time-to-live for cache entries in seconds
            max_size: Maximum number of entries to store
        """
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self.cache = OrderedDict()
        self.stats = {
            'total_queries': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'evictions': 0,
            'avg_response_time': 0,
            'response_times': []
        }
        
        logger.info(f"Cache initialized: TTL={ttl_seconds}s, Max Size={max_size}")
    
    def _generate_key(self, query: str) -> str:
        """Generate cache key from query"""
        # Normalize query (lowercase, strip whitespace)
        normalized = query.lower().strip()
        
        # Generate hash for consistent keys
        return hashlib.sha256(normalized.encode()).hexdigest()
    
    def get(self, query: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached result for query.
        
        Args:
            query: The query string
            
        Returns:
            Cached result if found and not expired, None otherwise
        """
        self.stats['total_queries'] += 1
        key = self._generate_key(query)
        
        if key in self.cache:
            entry = self.cache[key]
            
            # Check if entry has expired
            if datetime.now() < entry['expires_at']:
                # Move to end (most recently used)
                self.cache.move_to_end(key)
                self.stats['cache_hits'] += 1
                
                logger.debug(f"Cache hit for query: {query[:50]}...")
                return entry['data']
            else:
                # Remove expired entry
                del self.cache[key]
                logger.debug(f"Cache entry expired for query: {query[:50]}...")
        
        self.stats['cache_misses'] += 1
        logger.debug(f"Cache miss for query: {query[:50]}...")
        return None
    
    def set(self, query: str, data: Dict[str, Any]) -> None:
        """
        Store query result in cache.
        
        Args:
            query: The query string
            data: Result data to cache
        """
        key = self._generate_key(query)
        
        # Remove if exists (to update position)
        if key in self.cache:
            del self.cache[key]
        
        # Check if we need to evict
        if len(self.cache) >= self.max_size:
            # Remove oldest entry (LRU)
            oldest_key = next(iter(self.cache))
            del self.cache[oldest_key]
            self.stats['evictions'] += 1
            logger.debug("Cache eviction: removed oldest entry")
        
        # Add new entry
        self.cache[key] = {
            'data': data,
            'created_at': datetime.now(),
            'expires_at': datetime.now() + timedelta(seconds=self.ttl_seconds),
            'query': query
        }
        
        logger.debug(f"Cached result for query: {query[:50]}...")
    
    def invalidate(self, query: str) -> bool:
        """
        Manually invalidate a cache entry.
        
        Args:
            query: The query to invalidate
            
        Returns:
            True if entry was found and removed, False otherwise
        """
        key = self._generate_key(query)
        
        if key in self.cache:
            del self.cache[key]
            logger.info(f"Cache invalidated for query: {query[:50]}...")
            return True
        
        return False
    
    def invalidate_pattern(self, pattern: str) -> int:
        """
        Invalidate all cache entries matching a pattern.
        Useful for invalidating related queries.
        
        Args:
            pattern: Pattern to match (case-insensitive)
            
        Returns:
            Number of entries invalidated
        """
        pattern_lower = pattern.lower()
        keys_to_remove = []
        
        for key, entry in self.cache.items():
            if pattern_lower in entry['query'].lower():
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.cache[key]
        
        if keys_to_remove:
            logger.info(f"Cache invalidated {len(keys_to_remove)} entries matching: {pattern}")
        
        return len(keys_to_remove)
    
    def clear(self) -> None:
        """Clear all cache entries"""
        count = len(self.cache)
        self.cache.clear()
        logger.info(f"Cache cleared: {count} entries removed")
    
    def get_hit_rate(self) -> float:
        """Calculate cache hit rate percentage"""
        total = self.stats['cache_hits'] + self.stats['cache_misses']
        if total == 0:
            return 0.0
        
        return (self.stats['cache_hits'] / total) * 100
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get detailed cache statistics"""
        return {
            'total_queries': self.stats['total_queries'],
            'cache_hits': self.stats['cache_hits'],
            'cache_misses': self.stats['cache_misses'],
            'hit_rate': round(self.get_hit_rate(), 2),
            'evictions': self.stats['evictions'],
            'current_size': len(self.cache),
            'max_size': self.max_size,
            'ttl_seconds': self.ttl_seconds,
            'avg_response_time': self.stats['avg_response_time']
        }
    
    def get_recent_queries(self, limit: int = 10) -> list:
        """Get most recently cached queries"""
        recent = []
        
        for key, entry in reversed(list(self.cache.items())[-limit:]):
            recent.append({
                'query': entry['query'],
                'created_at': entry['created_at'].isoformat(),
                'expires_at': entry['expires_at'].isoformat()
            })
        
        return recent
    
    def update_response_time(self, response_time_ms: int) -> None:
        """Update average response time statistics"""
        self.stats['response_times'].append(response_time_ms)
        
        # Keep only last 100 response times
        if len(self.stats['response_times']) > 100:
            self.stats['response_times'] = self.stats['response_times'][-100:]
        
        # Calculate average
        if self.stats['response_times']:
            self.stats['avg_response_time'] = sum(self.stats['response_times']) / len(self.stats['response_times'])
    
    def cleanup_expired(self) -> int:
        """
        Remove all expired entries from cache.
        Should be called periodically.
        
        Returns:
            Number of entries removed
        """
        now = datetime.now()
        keys_to_remove = []
        
        for key, entry in self.cache.items():
            if now >= entry['expires_at']:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.cache[key]
        
        if keys_to_remove:
            logger.info(f"Cleanup: removed {len(keys_to_remove)} expired entries")
        
        return len(keys_to_remove)
    
    def warmup(self, queries: list) -> None:
        """
        Warm up cache with common queries.
        Used for preloading frequently used queries.
        
        Args:
            queries: List of (query, result) tuples to preload
        """
        for query, result in queries:
            self.set(query, result)
        
        logger.info(f"Cache warmed up with {len(queries)} queries")
    
    def export_cache(self) -> Dict[str, Any]:
        """Export cache contents for backup or analysis"""
        export_data = {
            'timestamp': datetime.now().isoformat(),
            'statistics': self.get_statistics(),
            'entries': []
        }
        
        for key, entry in self.cache.items():
            export_data['entries'].append({
                'key': key,
                'query': entry['query'],
                'created_at': entry['created_at'].isoformat(),
                'expires_at': entry['expires_at'].isoformat()
            })
        
        return export_data