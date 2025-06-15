'use client'
import React, { useState } from 'react';
import { loadParquetFromUrl, queryTable } from '@/lib/duckdb-wasm';
import * as duckdb from '@duckdb/duckdb-wasm';

interface ParquetLoaderState {
    loading: boolean;
    error: string | null;
    success: string | null;
    rowCount: number;
    database: duckdb.AsyncDuckDB | null;
    connection: duckdb.AsyncDuckDBConnection | null;
    tableName: string;
}

export default function ParquetLoader() {
    const [url, setUrl] = useState('');
    const [tableName, setTableName] = useState('episode');
    const [state, setState] = useState<ParquetLoaderState>({
        loading: false,
        error: null,
        success: null,
        rowCount: 0,
        database: null,
        connection: null,
        tableName: ''
    });

    const [queryInput, setQueryInput] = useState('');
    const [queryResult, setQueryResult] = useState<any[]>([]);
    const [queryLoading, setQueryLoading] = useState(false);

    const sampleQueries = [
        `SELECT * FROM ${tableName} LIMIT 10`,
        `SELECT COUNT(*) as total_rows FROM ${tableName}`,
        `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '${tableName}'`
    ];

    const handleLoadParquet = async (e: React.FormEvent) => {
        e.preventDefault();

        if (!url.trim()) {
            setState(prev => ({ ...prev, error: 'URL is required' }));
            return;
        }

        setState(prev => ({
            ...prev,
            loading: true,
            error: null,
            success: null
        }));

        try {
            const result = await loadParquetFromUrl(url, tableName);

            setState(prev => ({
                ...prev,
                loading: false,
                success: result.message,
                rowCount: result.rowCount,
                database: result.database,
                connection: result.connection,
                tableName: result.tableName
            }));

        } catch (error) {
            setState(prev => ({
                ...prev,
                loading: false,
                error: error instanceof Error ? error.message : 'Unknown error occurred'
            }));
        }
    };

    const handleQuery = async (e: React.FormEvent) => {
        e.preventDefault();

        if (!state.connection || !queryInput.trim()) {
            return;
        }

        setQueryLoading(true);
        setQueryResult([]);

        try {
            const result = await queryTable(state.connection, queryInput);
            setQueryResult(result);
        } catch (error) {
            setState(prev => ({
                ...prev,
                error: error instanceof Error ? error.message : 'Query failed'
            }));
        } finally {
            setQueryLoading(false);
        }
    };

    const handleCleanup = async () => {
        if (state.connection) {
            try {
                await state.connection.close();
                setState(prev => ({
                    ...prev,
                    connection: null,
                    database: null,
                    success: null,
                    rowCount: 0,
                    tableName: ''
                }));
                setQueryResult([]);
            } catch (error) {
                console.error('Error closing connection:', error);
            }
        }
    };

    return (
        <div className="flex h-screen max-h-screen max-w-full bg-slate-950 text-gray-200">
            <div className="bg-slate-900 p-6 flex-none w-200">
                <h2 className="text-2xl font-bold mb-4">Load Parquet File with DuckDB-WASM</h2>

                <form onSubmit={handleLoadParquet} className="space-y-4">
                    <div>
                        <label htmlFor="url" className="block text-sm font-medium text-gray-700 mb-1">
                            Parquet File URL
                        </label>
                        <input
                            type="url"
                            id="url"
                            value={url}
                            onChange={(e) => setUrl(e.target.value)}
                            className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                            placeholder="https://huggingface.co/datasets/lerobot/columbia_cairlab_pusht_real/resolve/main/data/chunk-000/episode_000000.parquet"
                            disabled={state.loading}
                        />
                    </div>

                    <div>
                        <label htmlFor="tableName" className="block text-sm font-medium text-gray-700 mb-1">
                            Table Name
                        </label>
                        <input
                            type="text"
                            id="tableName"
                            value={tableName}
                            onChange={(e) => setTableName(e.target.value)}
                            className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                            placeholder="episode"
                            disabled={state.loading}
                        />
                    </div>

                    <div className="flex space-x-3">
                        <button
                            type="submit"
                            disabled={state.loading}
                            className="px-4 py-2 bg-red-500 text-white rounded-md hover:bg-green-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                            {state.loading ? 'Loading...' : 'Load Parquet'}
                        </button>

                        {state.connection && (
                            <button
                                type="button"
                                onClick={handleCleanup}
                                className="px-4 py-2 bg-gray-500 text-white rounded-md hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-offset-2"
                            >
                                Close Connection
                            </button>
                        )}


                    </div>
                    <p className="text-sm text-gray-600">
                        Example:
                    </p>
                    <p className="text-sm text-gray-600">
                        https://huggingface.co/datasets/lerobot/columbia_cairlab_pusht_real/resolve/main/data/chunk-000/episode_000000.parquet
                    </p>
                </form>

                {/* Status Messages */}
                {state.error && (
                    <div className="mt-4 p-3 bg-red-100 border border-red-400 text-red-700 rounded">
                        {state.error}
                    </div>
                )}

                {state.success && (
                    <div className="mt-4 p-3 bg-green-100 border border-green-400 text-green-700 rounded">
                        {state.success} - Loaded {state.rowCount} rows into table "{state.tableName}"
                    </div>
                )}
            </div>

            {/* Query Section */}
            {state.connection && (
                <div className="bg-slate-950 p-6 flex-1 max-w-2/3">
                    <h3 className="text-xl font-bold mb-4">Query Data</h3>

                    <form onSubmit={handleQuery} className="space-y-4">
                        <div>
                            <label htmlFor="query" className="block text-sm font-medium text-gray-700 mb-1">
                                SQL Query
                            </label>

                            <textarea
                                id="query"
                                value={queryInput}
                                onChange={(e) => setQueryInput(e.target.value)}
                                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                rows={3}
                                placeholder={`SELECT * FROM ${state.tableName} LIMIT 10`}
                                disabled={queryLoading}
                            />
                        </div>

                        <div className="mb-3">
                            <p className="text-sm text-gray-600 mb-2">Sample queries:</p>
                            <div className="flex flex-wrap gap-2">
                                {sampleQueries.map((sampleQuery, index) => (
                                    <button
                                        key={index}
                                        onClick={() => setQueryInput(sampleQuery)}
                                        className="text-xs bg-blue-100 text-blue-800 px-2 py-1 rounded hover:bg-blue-200"
                                    >
                                        {sampleQuery.length > 40 ? sampleQuery.substring(0, 40) + '...' : sampleQuery}
                                    </button>
                                ))}
                            </div>
                        </div>

                        <button
                            type="submit"
                            disabled={queryLoading || !queryInput.trim()}
                            className="px-4 py-2 bg-red-500 text-white rounded-md hover:bg-green-400 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                            {queryLoading ? 'Running Query...' : 'Execute Query'}
                        </button>
                    </form>

                    {/* Query Results */}
                    {queryResult.length > 0 && (
                        <div className="mt-6 ">
                            <h4 className="text-lg font-semibold mb-3">Query Results ({queryResult.length} rows)</h4>
                            <div className="overflow-auto max-w-fit">
                                <table className="bg-slate-900 border border-gray-200">
                                    <thead className="bg-slate-800">
                                        <tr>
                                            {Object.keys(queryResult[0]).map((key) => (
                                                <th key={key} className="px-4 py-2 text-left text-md font-medium text-gray-500 uppercase tracking-wider border-b">
                                                    {key}
                                                </th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-gray-200">
                                        {queryResult.slice(0, 100).map((row, index) => (
                                            <tr key={index} className="hover:bg-green-200">
                                                {Object.values(row).map((value: any, cellIndex) => (
                                                    <td key={cellIndex} className="px-4 py-2 text-md text-white border-b">
                                                        {value !== null && value !== undefined ? String(value) : 'NULL'}
                                                    </td>
                                                ))}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                                {queryResult.length > 100 && (
                                    <p className="mt-2 text-sm text-gray-500">
                                        Showing first 100 of {queryResult.length} rows
                                    </p>
                                )}
                            </div>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}