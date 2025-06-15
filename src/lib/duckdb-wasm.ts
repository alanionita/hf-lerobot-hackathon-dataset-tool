import * as duckdb from '@duckdb/duckdb-wasm';

const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

// Select a bundle based on browser checks
const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

const worker_url = URL.createObjectURL(
  new Blob([`importScripts("${bundle.mainWorker!}");`], { type: 'text/javascript' })
);

let db: duckdb.AsyncDuckDB | null = null;

export async function initDuckDBWasm(): Promise<duckdb.AsyncDuckDB> {
  if (db) return db;

  try {
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(worker_url);
    return db
  } catch (error) {
    console.error('Failed to initialize DuckDB-WASM:', error);
    throw error;
  }
}

export async function loadParquetFromUrl(url: string, tableName: string = 'dataset') {
  let connection: duckdb.AsyncDuckDBConnection | null = null;

  try {
    if (!url) {
      throw new Error('URL is required');
    }

    const database = await initDuckDBWasm();
    connection = await database.connect();

    // Register the remote parquet file
    await database.registerFileURL(tableName + '.parquet', url, duckdb.DuckDBDataProtocol.HTTP, true);

    // Create table from parquet file
    const createTableQuery = `
      CREATE OR REPLACE TABLE ${tableName} AS 
      SELECT * FROM read_parquet('${tableName}.parquet')
    `;

    // console.log('Executing query:', createTableQuery);
    await connection.query(createTableQuery);

    // Get basic info about the loaded table
    const infoQuery = `SELECT COUNT(*) as row_count FROM ${tableName}`;

    const result = await connection.query(infoQuery);

    // Convert Arrow table to JavaScript objects
    const rows = result.toArray().map(row => row.toJSON());

    return {
      message: 'Parquet file loaded successfully',
      tableName,
      rowCount: rows[0]?.row_count || 0,
      database,
      connection
    };

  } catch (error) {
    console.error('Error loading parquet:', error);

    // Clean up connection on error
    if (connection) {
      try {
        await connection.close();
      } catch (closeError) {
        console.error('Error closing connection:', closeError);
      }
    }

    throw new Error(
      error instanceof Error
        ? `Failed to load parquet file: ${error.message}`
        : 'Failed to load parquet file: Unknown error'
    );
  }
}

// Utility function to query the loaded data
export async function queryTable(
  connection: duckdb.AsyncDuckDBConnection,
  query: string
): Promise<any[]> {
  try {
    const result = await connection.query(query);
    return result.toArray().map(row => row.toJSON());
  } catch (error) {
    console.error('Query error:', error);
    throw error;
  }
}