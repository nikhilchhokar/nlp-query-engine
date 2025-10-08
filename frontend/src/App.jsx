import React, { useState, useEffect } from 'react';
import { Search, Database, Upload, FileText, Check, X, Loader, Clock, TrendingUp, Users, Server } from 'lucide-react';

const NLPQueryEngine = () => {
  const [activeTab, setActiveTab] = useState('connect');
  const [dbConnected, setDbConnected] = useState(false);
  const [connectionString, setConnectionString] = useState('postgresql://user:pass@localhost:5432/company_db');
  const [schema, setSchema] = useState(null);
  const [documents, setDocuments] = useState([]);
  const [uploadProgress, setUploadProgress] = useState({});
  const [query, setQuery] = useState('');
  const [results, setResults] = useState(null);
  const [queryHistory, setQueryHistory] = useState([]);
  const [loading, setLoading] = useState(false);
  const [metrics, setMetrics] = useState({
    totalQueries: 0,
    avgResponseTime: 0,
    cacheHitRate: 0,
    activeConnections: 1
  });

  // Simulated API calls - Replace with actual backend calls
  const connectDatabase = async () => {
    setLoading(true);
    try {
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 1500));
      
      const mockSchema = {
        tables: [
          {
            name: 'employees',
            columns: [
              { name: 'emp_id', type: 'integer', primary_key: true },
              { name: 'full_name', type: 'varchar', nullable: false },
              { name: 'dept_id', type: 'integer', foreign_key: 'departments.dept_id' },
              { name: 'position', type: 'varchar', nullable: false },
              { name: 'annual_salary', type: 'decimal', nullable: false },
              { name: 'join_date', type: 'date', nullable: false },
              { name: 'office_location', type: 'varchar', nullable: true }
            ],
            row_count: 247
          },
          {
            name: 'departments',
            columns: [
              { name: 'dept_id', type: 'integer', primary_key: true },
              { name: 'dept_name', type: 'varchar', nullable: false },
              { name: 'manager_id', type: 'integer', foreign_key: 'employees.emp_id' }
            ],
            row_count: 8
          },
          {
            name: 'performance_reviews',
            columns: [
              { name: 'review_id', type: 'integer', primary_key: true },
              { name: 'emp_id', type: 'integer', foreign_key: 'employees.emp_id' },
              { name: 'review_date', type: 'date', nullable: false },
              { name: 'rating', type: 'integer', nullable: false },
              { name: 'comments', type: 'text', nullable: true }
            ],
            row_count: 1205
          }
        ],
        relationships: [
          { from: 'employees.dept_id', to: 'departments.dept_id' },
          { from: 'departments.manager_id', to: 'employees.emp_id' },
          { from: 'performance_reviews.emp_id', to: 'employees.emp_id' }
        ]
      };
      
      setSchema(mockSchema);
      setDbConnected(true);
    } catch (error) {
      alert('Failed to connect to database');
    } finally {
      setLoading(false);
    }
  };

  const uploadDocuments = async (files) => {
    const fileArray = Array.from(files);
    
    for (const file of fileArray) {
      const fileId = `${file.name}-${Date.now()}`;
      setUploadProgress(prev => ({ ...prev, [fileId]: 0 }));
      
      // Simulate upload progress
      for (let i = 0; i <= 100; i += 20) {
        await new Promise(resolve => setTimeout(resolve, 200));
        setUploadProgress(prev => ({ ...prev, [fileId]: i }));
      }
      
      setDocuments(prev => [...prev, {
        id: fileId,
        name: file.name,
        size: file.size,
        type: file.type,
        uploadedAt: new Date().toISOString()
      }]);
      
      setUploadProgress(prev => {
        const newProgress = { ...prev };
        delete newProgress[fileId];
        return newProgress;
      });
    }
  };

  const executeQuery = async () => {
    if (!query.trim()) return;

    setLoading(true);
    const startTime = Date.now();

    try {
      const response = await fetch('http://localhost:8000/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query })
      });

      if (!response.ok) {
        throw new Error('Query failed');
      }

      const resultData = await response.json();

      setResults({
        ...resultData,
        cache_hit: resultData.cache_hit || false,
        response_time_ms: Date.now() - startTime
      });

      setQueryHistory(prev => [{
        query,
        timestamp: new Date().toISOString(),
        responseTime: Date.now() - startTime,
        cacheHit: resultData.cache_hit || false
      }, ...prev.slice(0, 9)]);

      setMetrics(prev => ({
        totalQueries: prev.totalQueries + 1,
        avgResponseTime: Math.round((prev.avgResponseTime * prev.totalQueries + (Date.now() - startTime)) / (prev.totalQueries + 1)),
        cacheHitRate: prev.cacheHitRate,
        activeConnections: prev.activeConnections
      }));
    } catch (error) {
      alert('Query execution failed');
    } finally {
      setLoading(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    const files = e.dataTransfer.files;
    if (files.length > 0) {
      uploadDocuments(files);
    }
  };

  const handleDragOver = (e) => {
    e.preventDefault();
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-blue-900 to-gray-900 text-white">
      {/* Header */}
      <header className="border-b border-gray-700 bg-gray-900/50 backdrop-blur">
        <div className="container mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <Database className="w-8 h-8 text-blue-400" />
              <h1 className="text-2xl font-bold">NLP Query Engine</h1>
            </div>
            <div className="flex items-center space-x-6">
              <div className="flex items-center space-x-2 text-sm">
                <Server className={`w-4 h-4 ${dbConnected ? 'text-green-400' : 'text-gray-500'}`} />
                <span>{dbConnected ? 'Connected' : 'Not Connected'}</span>
              </div>
              <div className="flex items-center space-x-2 text-sm">
                <FileText className="w-4 h-4 text-blue-400" />
                <span>{documents.length} Documents</span>
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="container mx-auto px-6 py-8">
        {/* Metrics Dashboard */}
        <div className="grid grid-cols-4 gap-4 mb-8">
          <div className="bg-gray-800/50 backdrop-blur rounded-lg p-4 border border-gray-700">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-400">Total Queries</p>
                <p className="text-2xl font-bold mt-1">{metrics.totalQueries}</p>
              </div>
              <TrendingUp className="w-8 h-8 text-blue-400" />
            </div>
          </div>
          <div className="bg-gray-800/50 backdrop-blur rounded-lg p-4 border border-gray-700">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-400">Avg Response Time</p>
                <p className="text-2xl font-bold mt-1">{metrics.avgResponseTime}ms</p>
              </div>
              <Clock className="w-8 h-8 text-green-400" />
            </div>
          </div>
          <div className="bg-gray-800/50 backdrop-blur rounded-lg p-4 border border-gray-700">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-400">Cache Hit Rate</p>
                <p className="text-2xl font-bold mt-1">{metrics.cacheHitRate}%</p>
              </div>
              <Database className="w-8 h-8 text-purple-400" />
            </div>
          </div>
          <div className="bg-gray-800/50 backdrop-blur rounded-lg p-4 border border-gray-700">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-400">Active Connections</p>
                <p className="text-2xl font-bold mt-1">{metrics.activeConnections}</p>
              </div>
              <Users className="w-8 h-8 text-orange-400" />
            </div>
          </div>
        </div>

        {/* Tab Navigation */}
        <div className="flex space-x-4 mb-6">
          <button
            onClick={() => setActiveTab('connect')}
            className={`px-6 py-3 rounded-lg font-medium transition-all ${
              activeTab === 'connect'
                ? 'bg-blue-600 text-white shadow-lg'
                : 'bg-gray-800/50 text-gray-300 hover:bg-gray-700'
            }`}
          >
            Connect Data
          </button>
          <button
            onClick={() => setActiveTab('query')}
            className={`px-6 py-3 rounded-lg font-medium transition-all ${
              activeTab === 'query'
                ? 'bg-blue-600 text-white shadow-lg'
                : 'bg-gray-800/50 text-gray-300 hover:bg-gray-700'
            }`}
          >
            Query Data
          </button>
        </div>

        {/* Connect Data Tab */}
        {activeTab === 'connect' && (
          <div className="grid grid-cols-2 gap-6">
            {/* Database Connection */}
            <div className="bg-gray-800/50 backdrop-blur rounded-lg p-6 border border-gray-700">
              <h2 className="text-xl font-bold mb-4 flex items-center">
                <Database className="w-6 h-6 mr-2 text-blue-400" />
                Database Connection
              </h2>
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium mb-2">Connection String</label>
                  <input
                    type="text"
                    value={connectionString}
                    onChange={(e) => setConnectionString(e.target.value)}
                    className="w-full px-4 py-2 bg-gray-900 border border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="postgresql://user:pass@localhost:5432/db"
                  />
                </div>
                <button
                  onClick={connectDatabase}
                  disabled={loading || dbConnected}
                  className="w-full py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-600 disabled:cursor-not-allowed rounded-lg font-medium transition-colors flex items-center justify-center"
                >
                  {loading ? (
                    <>
                      <Loader className="w-5 h-5 mr-2 animate-spin" />
                      Connecting...
                    </>
                  ) : dbConnected ? (
                    <>
                      <Check className="w-5 h-5 mr-2" />
                      Connected
                    </>
                  ) : (
                    'Connect & Analyze'
                  )}
                </button>
              </div>

              {/* Schema Visualization */}
              {schema && (
                <div className="mt-6 pt-6 border-t border-gray-700">
                  <h3 className="text-lg font-semibold mb-3">Discovered Schema</h3>
                  <div className="space-y-3">
                    {schema.tables.map((table, idx) => (
                      <div key={idx} className="bg-gray-900/50 rounded-lg p-4">
                        <div className="flex items-center justify-between mb-2">
                          <span className="font-medium text-blue-400">{table.name}</span>
                          <span className="text-sm text-gray-400">{table.row_count} rows</span>
                        </div>
                        <div className="space-y-1">
                          {table.columns.slice(0, 4).map((col, colIdx) => (
                            <div key={colIdx} className="text-sm flex items-center">
                              <span className="text-gray-300">{col.name}</span>
                              <span className="text-gray-500 ml-2">({col.type})</span>
                              {col.primary_key && (
                                <span className="ml-2 px-2 py-0.5 bg-yellow-600/20 text-yellow-400 rounded text-xs">PK</span>
                              )}
                              {col.foreign_key && (
                                <span className="ml-2 px-2 py-0.5 bg-purple-600/20 text-purple-400 rounded text-xs">FK</span>
                              )}
                            </div>
                          ))}
                          {table.columns.length > 4 && (
                            <div className="text-sm text-gray-500">+ {table.columns.length - 4} more columns</div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                  <div className="mt-4 text-sm text-gray-400">
                    Found {schema.tables.length} tables, {schema.tables.reduce((sum, t) => sum + t.columns.length, 0)} columns, {schema.relationships.length} relationships
                  </div>
                </div>
              )}
            </div>

            {/* Document Upload */}
            <div className="bg-gray-800/50 backdrop-blur rounded-lg p-6 border border-gray-700">
              <h2 className="text-xl font-bold mb-4 flex items-center">
                <Upload className="w-6 h-6 mr-2 text-green-400" />
                Document Upload
              </h2>
              
              <div
                onDrop={handleDrop}
                onDragOver={handleDragOver}
                className="border-2 border-dashed border-gray-600 rounded-lg p-8 text-center hover:border-blue-500 transition-colors cursor-pointer"
              >
                <Upload className="w-12 h-12 mx-auto mb-4 text-gray-500" />
                <p className="text-gray-300 mb-2">Drag and drop files here</p>
                <p className="text-sm text-gray-500">or click to browse</p>
                <input
                  type="file"
                  multiple
                  onChange={(e) => uploadDocuments(e.target.files)}
                  className="hidden"
                  id="file-upload"
                />
                <label
                  htmlFor="file-upload"
                  className="inline-block mt-4 px-6 py-2 bg-green-600 hover:bg-green-700 rounded-lg cursor-pointer transition-colors"
                >
                  Select Files
                </label>
              </div>

              {/* Upload Progress */}
              {Object.keys(uploadProgress).length > 0 && (
                <div className="mt-4 space-y-2">
                  {Object.entries(uploadProgress).map(([fileId, progress]) => (
                    <div key={fileId} className="bg-gray-900/50 rounded-lg p-3">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-sm truncate">{fileId.split('-')[0]}</span>
                        <span className="text-sm text-gray-400">{progress}%</span>
                      </div>
                      <div className="w-full bg-gray-700 rounded-full h-2">
                        <div
                          className="bg-green-500 h-2 rounded-full transition-all"
                          style={{ width: `${progress}%` }}
                        />
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {/* Uploaded Documents */}
              {documents.length > 0 && (
                <div className="mt-6 pt-6 border-t border-gray-700">
                  <h3 className="text-lg font-semibold mb-3">Uploaded Documents</h3>
                  <div className="space-y-2 max-h-60 overflow-y-auto">
                    {documents.map((doc) => (
                      <div key={doc.id} className="bg-gray-900/50 rounded-lg p-3 flex items-center justify-between">
                        <div className="flex items-center space-x-3">
                          <FileText className="w-5 h-5 text-blue-400" />
                          <div>
                            <p className="text-sm font-medium">{doc.name}</p>
                            <p className="text-xs text-gray-500">
                              {(doc.size / 1024).toFixed(1)} KB
                            </p>
                          </div>
                        </div>
                        <Check className="w-5 h-5 text-green-400" />
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Query Data Tab */}
        {activeTab === 'query' && (
          <div className="space-y-6">
            {/* Query Input */}
            <div className="bg-gray-800/50 backdrop-blur rounded-lg p-6 border border-gray-700">
              <h2 className="text-xl font-bold mb-4 flex items-center">
                <Search className="w-6 h-6 mr-2 text-purple-400" />
                Natural Language Query
              </h2>
              <div className="flex space-x-4">
                <input
                  type="text"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  onKeyPress={(e) => e.key === 'Enter' && executeQuery()}
                  className="flex-1 px-4 py-3 bg-gray-900 border border-gray-600 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                  placeholder="e.g., Show me all Python developers in Engineering earning over 100k"
                />
                <button
                  onClick={executeQuery}
                  disabled={loading || !dbConnected}
                  className="px-8 py-3 bg-purple-600 hover:bg-purple-700 disabled:bg-gray-600 disabled:cursor-not-allowed rounded-lg font-medium transition-colors flex items-center"
                >
                  {loading ? (
                    <>
                      <Loader className="w-5 h-5 mr-2 animate-spin" />
                      Searching...
                    </>
                  ) : (
                    <>
                      <Search className="w-5 h-5 mr-2" />
                      Query
                    </>
                  )}
                </button>
              </div>

              {/* Query History */}
              {queryHistory.length > 0 && (
                <div className="mt-4">
                  <p className="text-sm text-gray-400 mb-2">Recent Queries:</p>
                  <div className="flex flex-wrap gap-2">
                    {queryHistory.slice(0, 5).map((item, idx) => (
                      <button
                        key={idx}
                        onClick={() => setQuery(item.query)}
                        className="px-3 py-1 bg-gray-700/50 hover:bg-gray-700 rounded-full text-sm transition-colors"
                      >
                        {item.query.slice(0, 40)}...
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>

            {/* Results */}
            {results && (
              <div className="space-y-6">
                {/* SQL Results */}
                {results.sql_results && (
                  <div className="bg-gray-800/50 backdrop-blur rounded-lg p-6 border border-gray-700">
                    <div className="flex items-center justify-between mb-4">
                      <h3 className="text-lg font-semibold flex items-center">
                        <Database className="w-5 h-5 mr-2 text-blue-400" />
                        Database Results
                      </h3>
                      <div className="flex items-center space-x-4 text-sm">
                        <span className={`px-3 py-1 rounded-full ${results.cache_hit ? 'bg-green-600/20 text-green-400' : 'bg-orange-600/20 text-orange-400'}`}>
                          {results.cache_hit ? 'Cache Hit' : 'Cache Miss'}
                        </span>
                        <span className="text-gray-400">{results.response_time_ms}ms</span>
                      </div>
                    </div>
                    <div className="overflow-x-auto">
                      <table className="w-full">
                        <thead>
                          <tr className="border-b border-gray-700">
                            {results.sql_results.columns.map((col, idx) => (
                              <th key={idx} className="px-4 py-2 text-left text-sm font-medium text-gray-300">
                                {col}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {results.sql_results.rows.map((row, rowIdx) => (
                            <tr key={rowIdx} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                              {row.map((cell, cellIdx) => (
                                <td key={cellIdx} className="px-4 py-3 text-sm">
                                  {cell}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}

                {/* Document Results */}
                {results.document_results && results.document_results.length > 0 && (
                  <div className="bg-gray-800/50 backdrop-blur rounded-lg p-6 border border-gray-700">
                    <h3 className="text-lg font-semibold mb-4 flex items-center">
                      <FileText className="w-5 h-5 mr-2 text-green-400" />
                      Document Matches
                    </h3>
                    <div className="space-y-4">
                      {results.document_results.map((doc, idx) => (
                        <div key={idx} className="bg-gray-900/50 rounded-lg p-4">
                          <div className="flex items-center justify-between mb-2">
                            <span className="font-medium text-green-400">{doc.doc_name}</span>
                            <span className="text-sm text-gray-400">
                              Relevance: {(doc.relevance_score * 100).toFixed(0)}%
                            </span>
                          </div>
                          <p className="text-sm text-gray-300">{doc.excerpt}</p>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default NLPQueryEngine;