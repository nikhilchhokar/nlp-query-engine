"""
Document Processor Service
Handles document ingestion, chunking, embedding generation, and semantic search
"""
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    np = None

import numpy as np
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime
import hashlib
import re
import io

logger = logging.getLogger(__name__)

class DocumentProcessor:
    """
    Processes various document types and enables semantic search.
    Features:
    - Multi-format support (PDF, DOCX, TXT, CSV)
    - Intelligent chunking based on document structure
    - Batch embedding generation
    - Fast similarity search
    """
    
    def __init__(self, embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"):
        self.documents = []
        self.chunks = []
        self.embeddings = []
        self.embedding_model_name = embedding_model
        self.embedding_model = None
        self.embedding_dim = 384  # Dimension for all-MiniLM-L6-v2
        
        # Initialize embedding model lazily
        self._init_embedding_model()
        
        # Document type handlers
        self.handlers = {
            'application/pdf': self._process_pdf,
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document': self._process_docx,
            'text/plain': self._process_txt,
            'text/csv': self._process_csv,
        }
    
    def _init_embedding_model(self):
        """Initialize embedding model (lazy loading)"""
        try:
            # Using a simple embedding simulation for demo
            # In production, use actual sentence-transformers
            from sentence_transformers import SentenceTransformer
            self.embedding_model = SentenceTransformer(self.embedding_model_name)
            logger.info(f"Loaded embedding model: {self.embedding_model_name}")
        except ImportError:
            logger.warning("sentence-transformers not available, using mock embeddings")
            self.embedding_model = None
    
    async def process_document(self, filename: str, content: bytes, content_type: str) -> Dict[str, Any]:
        """
        Process a single document: extract text, chunk, and generate embeddings.
        
        Args:
            filename: Name of the file
            content: File content as bytes
            content_type: MIME type of the file
            
        Returns:
            Processing result with document ID and stats
        """
        try:
            # Generate unique document ID
            doc_id = hashlib.md5(f"{filename}{datetime.now().isoformat()}".encode()).hexdigest()
            
            # Extract text based on file type
            handler = self.handlers.get(content_type, self._process_txt)
            text = handler(content)
            
            # Intelligent chunking
            chunks = self._dynamic_chunking(text, self._detect_document_type(filename, text))
            
            # Generate embeddings in batches
            chunk_embeddings = await self._generate_embeddings_batch(chunks)
            
            # Store document metadata
            document = {
                'id': doc_id,
                'filename': filename,
                'content_type': content_type,
                'processed_at': datetime.now().isoformat(),
                'num_chunks': len(chunks),
                'total_length': len(text)
            }
            
            self.documents.append(document)
            
            # Store chunks with embeddings
            for i, (chunk, embedding) in enumerate(zip(chunks, chunk_embeddings)):
                self.chunks.append({
                    'doc_id': doc_id,
                    'chunk_id': f"{doc_id}_{i}",
                    'text': chunk,
                    'embedding': embedding,
                    'chunk_index': i
                })
            
            logger.info(f"Processed document {filename}: {len(chunks)} chunks created")
            
            return {
                'success': True,
                'doc_id': doc_id,
                'chunks_created': len(chunks)
            }
            
        except Exception as e:
            logger.error(f"Document processing failed for {filename}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _process_pdf(self, content: bytes) -> str:
        """Extract text from PDF"""
        try:
            import PyPDF2
            pdf_file = io.BytesIO(content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
            
            return text
        except ImportError:
            logger.warning("PyPDF2 not available, using placeholder text")
            return "PDF content placeholder - install PyPDF2 for actual extraction"
    
    def _process_docx(self, content: bytes) -> str:
        """Extract text from DOCX"""
        try:
            import docx
            doc = docx.Document(io.BytesIO(content))
            return "\n".join([paragraph.text for paragraph in doc.paragraphs])
        except ImportError:
            logger.warning("python-docx not available, using placeholder text")
            return "DOCX content placeholder - install python-docx for actual extraction"
    
    def _process_txt(self, content: bytes) -> str:
        """Extract text from plain text file"""
        try:
            return content.decode('utf-8')
        except UnicodeDecodeError:
            return content.decode('latin-1')
    
    def _process_csv(self, content: bytes) -> str:
        """Extract text from CSV"""
        try:
            import csv
            text = content.decode('utf-8')
            reader = csv.DictReader(io.StringIO(text))
            
            # Convert CSV to readable text
            rows = []
            for row in reader:
                row_text = ", ".join([f"{k}: {v}" for k, v in row.items()])
                rows.append(row_text)
            
            return "\n".join(rows)
        except Exception as e:
            logger.error(f"CSV processing error: {str(e)}")
            return self._process_txt(content)
    
    def _detect_document_type(self, filename: str, text: str) -> str:
        """
        Detect document type for optimal chunking strategy.
        
        Returns:
            'resume', 'contract', 'review', 'report', or 'general'
        """
        filename_lower = filename.lower()
        text_lower = text.lower()
        
        # Resume detection
        if any(term in filename_lower for term in ['resume', 'cv']) or \
           any(term in text_lower for term in ['education', 'experience', 'skills', 'qualifications']):
            return 'resume'
        
        # Contract detection
        if 'contract' in filename_lower or \
           any(term in text_lower for term in ['agreement', 'terms and conditions', 'party', 'clause']):
            return 'contract'
        
        # Review detection
        if any(term in filename_lower for term in ['review', 'evaluation', 'performance']) or \
           any(term in text_lower for term in ['performance', 'rating', 'feedback', 'evaluation']):
            return 'review'
        
        return 'general'
    
    def _dynamic_chunking(self, content: str, doc_type: str) -> List[str]:
        """
        Intelligent chunking based on document structure.
        
        Different strategies for different document types:
        - Resumes: Keep sections (education, experience, skills) together
        - Contracts: Preserve clause boundaries
        - Reviews: Maintain paragraph integrity
        - General: Semantic chunking with overlap
        """
        if doc_type == 'resume':
            return self._chunk_resume(content)
        elif doc_type == 'contract':
            return self._chunk_contract(content)
        elif doc_type == 'review':
            return self._chunk_review(content)
        else:
            return self._chunk_general(content)
    
    def _chunk_resume(self, content: str) -> List[str]:
        """Chunk resume by sections"""
        # Common resume sections
        section_headers = [
            'education', 'experience', 'skills', 'work experience',
            'professional experience', 'certifications', 'projects',
            'summary', 'objective', 'qualifications'
        ]
        
        chunks = []
        current_chunk = []
        
        for line in content.split('\n'):
            line_lower = line.lower().strip()
            
            # Check if line is a section header
            is_header = any(header in line_lower for header in section_headers)
            
            if is_header and current_chunk:
                # Start new chunk
                chunks.append('\n'.join(current_chunk))
                current_chunk = [line]
            else:
                current_chunk.append(line)
        
        # Add final chunk
        if current_chunk:
            chunks.append('\n'.join(current_chunk))
        
        return chunks if chunks else [content]
    
    def _chunk_contract(self, content: str) -> List[str]:
        """Chunk contract by clauses"""
        # Split by numbered clauses or articles
        pattern = r'(?:^|\n)(?:\d+\.|Article \d+|Section \d+|Clause \d+)'
        chunks = re.split(pattern, content)
        
        # Filter empty chunks and clean
        chunks = [chunk.strip() for chunk in chunks if chunk.strip()]
        
        return chunks if chunks else [content]
    
    def _chunk_review(self, content: str) -> List[str]:
        """Chunk review by paragraphs"""
        paragraphs = content.split('\n\n')
        chunks = [p.strip() for p in paragraphs if p.strip() and len(p.strip()) > 50]
        
        return chunks if chunks else [content]
    
    def _chunk_general(self, content: str, chunk_size: int = 512, overlap: int = 50) -> List[str]:
        """
        General semantic chunking with overlap.
        Tries to break at sentence boundaries.
        """
        sentences = re.split(r'(?<=[.!?])\s+', content)
        
        chunks = []
        current_chunk = []
        current_length = 0
        
        for sentence in sentences:
            sentence_length = len(sentence.split())
            
            if current_length + sentence_length > chunk_size and current_chunk:
                # Create chunk
                chunks.append(' '.join(current_chunk))
                
                # Keep last few sentences for overlap
                overlap_sentences = []
                overlap_length = 0
                for s in reversed(current_chunk):
                    s_len = len(s.split())
                    if overlap_length + s_len <= overlap:
                        overlap_sentences.insert(0, s)
                        overlap_length += s_len
                    else:
                        break
                
                current_chunk = overlap_sentences + [sentence]
                current_length = overlap_length + sentence_length
            else:
                current_chunk.append(sentence)
                current_length += sentence_length
        
        # Add final chunk
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        
        return chunks if chunks else [content]
    
    async def _generate_embeddings_batch(self, texts: List[str], batch_size: int = 32) -> List[np.ndarray]:
        """
        Generate embeddings in batches for efficiency.
        
        Args:
            texts: List of text chunks
            batch_size: Number of texts to process at once
            
        Returns:
            List of embedding vectors
        """
        if self.embedding_model is None:
            # Use mock embeddings for demo
            return [self._generate_mock_embedding(text) for text in texts]
        
        embeddings = []
        
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_embeddings = self.embedding_model.encode(batch, show_progress_bar=False)
            embeddings.extend(batch_embeddings)
        
        return embeddings
    
    def _generate_mock_embedding(self, text: str):
        """Generate mock embedding for testing"""
        if not NUMPY_AVAILABLE:
        # Return simple list instead
            return [0.1] * self.embedding_dim
        # Create deterministic embedding based on text
        hash_val = int(hashlib.md5(text.encode()).hexdigest(), 16)
        np.random.seed(hash_val % (2**32))
        return np.random.randn(self.embedding_dim).astype(np.float32)
    
    async def search_documents(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        Search documents using semantic similarity.
        
        Args:
            query: Search query
            top_k: Number of results to return
            
        Returns:
            List of relevant document chunks with metadata
        """
        if not self.chunks:
            return []
        
        try:
            # Generate query embedding
            if self.embedding_model:
                query_embedding = self.embedding_model.encode([query])[0]
            else:
                query_embedding = self._generate_mock_embedding(query)
            
            # Calculate similarities
            similarities = []
            for chunk in self.chunks:
                similarity = self._cosine_similarity(query_embedding, chunk['embedding'])
                similarities.append((similarity, chunk))
            
            # Sort by similarity and get top_k
            similarities.sort(key=lambda x: x[0], reverse=True)
            top_results = similarities[:top_k]
            
            # Format results
            results = []
            for similarity, chunk in top_results:
                doc = next((d for d in self.documents if d['id'] == chunk['doc_id']), None)
                
                results.append({
                    'doc_name': doc['filename'] if doc else 'Unknown',
                    'excerpt': self._create_excerpt(chunk['text']),
                    'relevance_score': float(similarity),
                    'chunk_index': chunk['chunk_index']
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Document search error: {str(e)}")
            return []
    
    def _cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """Calculate cosine similarity between two vectors"""
        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return dot_product / (norm1 * norm2)
    
    def _create_excerpt(self, text: str, max_length: int = 200) -> str:
        """Create a readable excerpt from chunk"""
        if len(text) <= max_length:
            return text
        
        # Try to break at sentence boundary
        excerpt = text[:max_length]
        last_period = excerpt.rfind('.')
        
        if last_period > max_length * 0.7:  # If we can break at a sentence
            return excerpt[:last_period + 1] + '...'
        
        return excerpt + '...'
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get document processing statistics"""
        return {
            'total_documents': len(self.documents),
            'total_chunks': len(self.chunks),
            'avg_chunks_per_doc': len(self.chunks) / len(self.documents) if self.documents else 0,
            'embedding_model': self.embedding_model_name
        }