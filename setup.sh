#!/bin/bash

# NLP Query Engine - Complete Setup Script
set -e

echo "================================================"
echo "NLP Query Engine - Setup"
echo "================================================"
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi
echo "✓ Python 3 found"

# Check Node.js
if ! command -v node &> /dev/null; then
    echo "Warning: Node.js not found. Frontend setup will be skipped."
    SKIP_FRONTEND=1
else
    echo "✓ Node.js found"
    SKIP_FRONTEND=0
fi

echo ""

# Create project structure
echo "Creating project structure..."
mkdir -p backend/api/routes backend/api/services backend/api/models backend/tests backend/logs backend/uploads
mkdir -p frontend/src/components frontend/public scripts docs

echo "✓ Project structure created"
echo ""

# Setup Python backend
echo "Setting up Python backend..."

# Create virtual environment
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip > /dev/null 2>&1
pip install -r requirements.txt

echo "✓ Python dependencies installed"
echo ""

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file..."
    cp .env.example .env
    echo "✓ .env file created"
else
    echo "✓ .env file already exists"
fi

# Create demo database
echo ""
echo "Creating demo database..."
python scripts/setup_demo_db.py

# Setup frontend
if [ $SKIP_FRONTEND -eq 0 ]; then
    echo ""
    echo "Setting up frontend..."
    cd frontend
    
    if [ ! -d "node_modules" ]; then
        npm install
        echo "✓ Frontend dependencies installed"
    else
        echo "✓ Frontend dependencies already installed"
    fi
    
    cd ..
fi

# Create run scripts
echo ""
echo "Creating run scripts..."

cat > run_backend.sh << 'BACKEND_EOF'
#!/bin/bash
source venv/bin/activate
cd backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000
BACKEND_EOF
chmod +x run_backend.sh

if [ $SKIP_FRONTEND -eq 0 ]; then
    cat > run_frontend.sh << 'FRONTEND_EOF'
#!/bin/bash
cd frontend
npm start
FRONTEND_EOF
    chmod +x run_frontend.sh
fi

echo "✓ Run scripts created"

echo ""
echo "================================================"
echo "Setup Complete!"
echo "================================================"
echo ""
echo "To start the application:"
echo "  Backend:  ./run_backend.sh"
if [ $SKIP_FRONTEND -eq 0 ]; then
    echo "  Frontend: ./run_frontend.sh"
fi
echo ""
echo "Or start both:"
echo "  Terminal 1: ./run_backend.sh"
echo "  Terminal 2: ./run_frontend.sh"
echo ""
echo "Access at:"
echo "  Frontend: http://localhost:3000"
echo "  Backend:  http://localhost:8000"
echo "  API Docs: http://localhost:8000/docs"
echo ""
echo "================================================"
