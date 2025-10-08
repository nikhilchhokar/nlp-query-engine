# NLP Query Engine for Employee Data

A production-ready natural language query system with dynamic schema discovery.

## Features

- Dynamic Schema Discovery - No hard-coding required
- Natural Language Processing - Plain English to SQL
- Document Intelligence - Process PDFs, DOCX, CSV
- High Performance - Query caching, <2s response times
- Production Ready - Error handling, logging, metrics

## Quick Start

### Prerequisites
- Python 3.8+
- Node.js 16+
- PostgreSQL/MySQL (or use SQLite)

### Installation

1. Clone repository
2. Install dependencies:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cd frontend && npm install && cd ..
```

3. Create demo database:
```bash
python scripts/setup_demo_db.py
```

4. Start services:
```bash
# Terminal 1 - Backend
cd backend
uvicorn main:app --reload

# Terminal 2 - Frontend  
cd frontend
npm start
```

5. Open http://localhost:3000

## Usage

1. Connect to your database
2. Upload employee documents
3. Ask questions in plain English!

## Documentation

See individual artifact files for complete implementation of:
- `backend/main.py` - FastAPI application
- `backend/api/services/schema_discovery.py` - Schema analysis
- `backend/api/services/query_engine.py` - Query processing
- `backend/api/services/document_processor.py` - Document handling
- `backend/api/services/cache_manager.py` - Caching system
- `frontend/src/App.jsx` - React UI

## Testing

```bash
pytest backend/tests/ -v
```

## License

MIT License
