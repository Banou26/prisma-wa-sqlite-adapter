import {
  ConnectionInfo,
  Debug,
  DriverAdapterError,
  IsolationLevel,
  SqlDriverAdapter,
  SqlDriverAdapterFactory,
  SqlQuery,
  SqlQueryable,
  SqlResultSet,
  Transaction,
  TransactionOptions,
  ColumnTypeEnum,
} from '@prisma/driver-adapter-utils'
import SQLiteESMFactory from 'wa-sqlite/dist/wa-sqlite.mjs'
import * as SQLite from 'wa-sqlite'

const debug = Debug('prisma:driver-adapter:wa-sqlite')

// Constants
const MAX_BIND_VALUES = 32766 // SQLite limit

// Type definitions for wa-sqlite
type WaSQLiteDB = number
type WaSQLiteStmt = any // wa-sqlite uses object with stmt property

interface WaSQLiteAdapter {
  sqlite3: any // wa-sqlite Factory result
  db: WaSQLiteDB
}

// Utility functions
function cleanArg(arg: unknown, argType?: unknown): unknown {
  if (arg === undefined) return null
  if (arg instanceof Date) return arg.toISOString()
  if (arg instanceof Uint8Array) return Array.from(arg)
  if (typeof arg === 'bigint') return Number(arg)
  return arg
}

function convertDriverError(error: Error): any {
  const errorMessage = error.message || ''
  
  // Check if it's a SQLiteError with a code
  if ('code' in error && typeof (error as any).code === 'number') {
    const code = (error as any).code

    // Map SQLite error codes to Prisma error kinds
    if (code === SQLite.SQLITE_CONSTRAINT || code === SQLite.SQLITE_CONSTRAINT_UNIQUE) {
      // Check if it's a unique constraint violation
      if (errorMessage.includes('UNIQUE') || errorMessage.includes('duplicate')) {
        return {
          kind: 'UniqueConstraintViolation',
          message: errorMessage,
        }
      }
      // Check if it's a foreign key constraint
      if (errorMessage.includes('FOREIGN KEY')) {
        return {
          kind: 'ForeignKeyConstraintViolation',
          message: errorMessage,
        }
      }
      return {
        kind: 'ConstraintViolation',
        message: errorMessage,
      }
    }

    if (code === SQLite.SQLITE_BUSY || code === SQLite.SQLITE_LOCKED) {
      return {
        kind: 'DatabaseTimeout',
        message: errorMessage,
      }
    }
  }

  // Check for transaction errors in message
  if (errorMessage.includes('no transaction is active')) {
    return {
      kind: 'TransactionAlreadyClosed',
      message: errorMessage,
    }
  }

  if (errorMessage.includes('cannot start a transaction within a transaction')) {
    return {
      kind: 'GenericDatabaseError',
      message: errorMessage,
    }
  }

  return {
    kind: 'GenericDatabaseError',
    message: errorMessage,
  }
}

function getColumnTypeEnums(columnNames: string[], rows: unknown[][]): Record<string, ColumnTypeEnum> {
  const columnTypes: Record<string, ColumnTypeEnum> = {}

  if (rows.length === 0) {
    // Default all columns to Text when no rows
    columnNames.forEach(name => {
      columnTypes[name] = ColumnTypeEnum.Text
    })
    return columnTypes
  }

  const firstRow = rows[0]
  if (!firstRow) {
    return columnTypes
  }

  columnNames.forEach((name, index) => {
    const value = firstRow[index]
    // Map to Prisma ColumnTypeEnum
    if (value === null || value === undefined) {
      columnTypes[name] = ColumnTypeEnum.Text // Default for NULL
    } else if (typeof value === 'number') {
      if (Number.isInteger(value)) {
        columnTypes[name] = ColumnTypeEnum.Int32
      } else {
        columnTypes[name] = ColumnTypeEnum.Double
      }
    } else if (typeof value === 'string') {
      columnTypes[name] = ColumnTypeEnum.Text
    } else if (typeof value === 'bigint') {
      columnTypes[name] = ColumnTypeEnum.Int64
    } else if (value instanceof Uint8Array || Array.isArray(value)) {
      columnTypes[name] = ColumnTypeEnum.Bytes
    } else if (value instanceof Date) {
      columnTypes[name] = ColumnTypeEnum.DateTime
    } else if (typeof value === 'boolean') {
      columnTypes[name] = ColumnTypeEnum.Boolean
    } else {
      columnTypes[name] = ColumnTypeEnum.Text // Default
    }
  })

  return columnTypes
}

function mapRow(row: unknown[], columnTypes: ColumnTypeEnum[]): unknown[] {
  return row.map((value, index) => {
    const type = columnTypes[index]

    if (value === null || value === undefined) {
      return null
    }

    // Handle blob data
    if (type === ColumnTypeEnum.Bytes && Array.isArray(value)) {
      return new Uint8Array(value)
    }

    // Handle dates stored as ISO strings
    if (type === ColumnTypeEnum.DateTime && typeof value === 'string') {
      // Check if it's a date string
      const dateRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z$/
      if (dateRegex.test(value)) {
        return new Date(value)
      }
    }

    // Handle bigint conversion
    if (type === ColumnTypeEnum.Int64 && typeof value === 'number') {
      return BigInt(value)
    }

    return value
  })
}

/**
 * wa-sqlite Queryable implementation - Base class for query execution
 */
class WaSQLiteQueryable implements SqlQueryable {
  readonly provider = 'sqlite'
  readonly adapterName = 'prisma-wa-sqlite-adapter'

  constructor(protected readonly adapter: WaSQLiteAdapter) {}

  /**
   * Execute a query given as SQL, interpolating the given parameters.
   */
  async queryRaw(query: SqlQuery): Promise<SqlResultSet> {
    const tag = '[js::query_raw]'
    debug(`${tag} %O`, query)

    const data = await this.performIO(query) as [string[], unknown[][]]
    return this.convertData(data)
  }

  private convertData(ioResult: [string[], unknown[][]]): SqlResultSet {
    const [columnNames, results] = ioResult

    if (results.length === 0) {
      // Still return column names even when no rows
      const columnTypes = Object.values(getColumnTypeEnums(columnNames, results))
      return {
        columnNames,
        columnTypes,
        rows: [],
      }
    }

    const columnTypes = Object.values(getColumnTypeEnums(columnNames, results))
    const rows = results.map((row) => mapRow(row, columnTypes))

    return {
      columnNames,
      columnTypes,
      rows,
    }
  }

  /**
   * Execute a query given as SQL, interpolating the given parameters and
   * returning the number of affected rows.
   */
  async executeRaw(query: SqlQuery): Promise<number> {
    const tag = '[js::execute_raw]'
    debug(`${tag} %O`, query)

    const result = await this.performIO(query, true)
    return result as number
  }

  protected async performIO(query: SqlQuery, executeRaw = false): Promise<[string[], unknown[][]] | number> {
    const { sqlite3, db } = this.adapter

    try {
      // Clean arguments
      const cleanedArgs = query.args.map((arg, i) => cleanArg(arg, query.argTypes?.[i]))

      // Use high-level API for better compatibility
      if (cleanedArgs.length === 0) {
        // No parameters - use exec
        if (executeRaw) {
          await sqlite3.exec(db, query.sql)
          return sqlite3.changes(db)
        } else {
          // For SELECT queries, use exec with callback
          const rows: unknown[][] = []
          const columnNames: string[] = []
          let firstRow = true

          await sqlite3.exec(db, query.sql, (row: unknown[], columns: string[]) => {
            if (firstRow) {
              columnNames.push(...columns)
              firstRow = false
            }
            rows.push(row)
          })

          return [columnNames, rows]
        }
      } else {
        // With parameters - use the prepared statement approach
        let stmt: any = null
        let str: number | null = null

        try {
          // Create string buffer for SQL
          str = sqlite3.str_new(db, query.sql)
          const prepared = await sqlite3.prepare_v2(db, sqlite3.str_value(str))

          if (!prepared || !prepared.stmt) {
            throw new Error(`Failed to prepare statement: ${query.sql}`)
          }

          stmt = prepared.stmt

          // Bind parameters
          cleanedArgs.forEach((arg, index) => {
            const paramIndex = index + 1 // SQLite uses 1-based indexing

            if (arg === null || arg === undefined) {
              sqlite3.bind_null(stmt, paramIndex)
            } else if (typeof arg === 'number') {
              if (Number.isInteger(arg)) {
                sqlite3.bind_int(stmt, paramIndex, arg)
              } else {
                sqlite3.bind_double(stmt, paramIndex, arg)
              }
            } else if (typeof arg === 'string') {
              sqlite3.bind_text(stmt, paramIndex, arg)
            } else if (arg instanceof Uint8Array) {
              sqlite3.bind_blob(stmt, paramIndex, arg)
            } else if (Array.isArray(arg)) {
              sqlite3.bind_blob(stmt, paramIndex, new Uint8Array(arg))
            } else {
              // Convert to string as fallback
              sqlite3.bind_text(stmt, paramIndex, String(arg))
            }
          })

          if (executeRaw) {
            // Execute and return affected rows
            const stepResult = await sqlite3.step(stmt)
            if (stepResult !== SQLite.SQLITE_DONE && stepResult !== SQLite.SQLITE_ROW) {
              throw new Error(`Failed to execute statement. Result code: ${stepResult}`)
            }
            return sqlite3.changes(db)
          } else {
            // Query and return results
            const columnNames: string[] = []
            const rows: unknown[][] = []

            // Get column names
            const columnCount = sqlite3.column_count(stmt)
            for (let i = 0; i < columnCount; i++) {
              columnNames.push(sqlite3.column_name(stmt, i))
            }

            // Fetch all rows
            let stepResult = await sqlite3.step(stmt)
            while (stepResult === SQLite.SQLITE_ROW) {
              const row: unknown[] = []
              for (let i = 0; i < columnCount; i++) {
                const type = sqlite3.column_type(stmt, i)

                switch (type) {
                  case SQLite.SQLITE_INTEGER:
                    row.push(sqlite3.column_int(stmt, i))
                    break
                  case SQLite.SQLITE_FLOAT:
                    row.push(sqlite3.column_double(stmt, i))
                    break
                  case SQLite.SQLITE_TEXT:
                    row.push(sqlite3.column_text(stmt, i))
                    break
                  case SQLite.SQLITE_BLOB:
                    row.push(sqlite3.column_blob(stmt, i))
                    break
                  case SQLite.SQLITE_NULL:
                  default:
                    row.push(null)
                    break
                }
              }
              rows.push(row)
              stepResult = await sqlite3.step(stmt)
            }

            if (stepResult !== SQLite.SQLITE_DONE) {
              throw new Error(`Error fetching results. Result code: ${stepResult}`)
            }

            return [columnNames, rows]
          }
        } finally {
          // Clean up
          if (stmt) {
            await sqlite3.finalize(stmt)
          }
          if (str !== null) {
            sqlite3.str_finish(str)
          }
        }
      }
    } catch (e) {
      console.error('[wa-sqlite] Error in performIO:', e)
      onError(e as Error)
    }
  }
}

/**
 * wa-sqlite Transaction implementation  
 */
class WaSQLiteTransaction extends WaSQLiteQueryable implements Transaction {
  constructor(
    adapter: WaSQLiteAdapter, 
    readonly options: TransactionOptions,
    private readonly parentAdapter: PrismaWaSQLiteAdapter
  ) {
    super(adapter)
  }

  async commit(): Promise<void> {
    debug(`[js::commit]`)
    await this.parentAdapter.commitTransaction()
  }

  async rollback(): Promise<void> {
    debug(`[js::rollback]`)
    await this.parentAdapter.rollbackTransaction()
  }
}


/**
 * Main wa-sqlite Prisma Adapter
 */
export class PrismaWaSQLiteAdapter extends WaSQLiteQueryable implements SqlDriverAdapter {
  readonly tags = {
    error: '[prisma:error]',
    warn: '[prisma:warn]',
    info: '[prisma:info]',
    query: '[prisma:query]',
  }
  
  private inTransaction = false
  private transactionDepth = 0
  private currentTransaction: WaSQLiteTransaction | null = null

  constructor(adapter: WaSQLiteAdapter, private readonly release?: () => Promise<void>) {
    super(adapter)
  }

  async executeScript(script: string): Promise<void> {
    try {
      const { sqlite3, db } = this.adapter
      debug('[wa-sqlite] Executing script:', script.substring(0, 100), '...')
      await sqlite3.exec(db, script)
    } catch (error) {
      console.error('[wa-sqlite] Error executing script:', error)
      onError(error as Error)
    }
  }

  getConnectionInfo(): ConnectionInfo {
    return {
      maxBindValues: MAX_BIND_VALUES,
      supportsRelationJoins: false,
    }
  }

  async startTransaction(isolationLevel?: IsolationLevel): Promise<Transaction> {
    if (isolationLevel && isolationLevel !== 'SERIALIZABLE') {
      throw new DriverAdapterError({
        kind: 'InvalidIsolationLevel',
        level: isolationLevel,
      })
    }

    const options: TransactionOptions = {
      usePhantomQuery: false,
    }

    const tag = '[js::startTransaction]'
    debug('%s inTransaction: %s, depth: %d', tag, this.inTransaction, this.transactionDepth)

    const { sqlite3, db } = this.adapter
    
    if (!this.inTransaction) {
      // Start a new transaction
      await sqlite3.exec(db, 'BEGIN')
      this.inTransaction = true
      this.transactionDepth = 1
      debug('%s Started new transaction', tag)
    } else {
      // Nested transaction - use savepoint
      this.transactionDepth++
      const savepointName = `prisma_savepoint_${this.transactionDepth}`
      await sqlite3.exec(db, `SAVEPOINT ${savepointName}`)
      debug('%s Created savepoint: %s', tag, savepointName)
    }
    
    // Create and return the transaction object
    const tx = new WaSQLiteTransaction(this.adapter, options, this)
    this.currentTransaction = tx
    return tx
  }

  async commitTransaction(): Promise<void> {
    const tag = '[js::commitTransaction]'
    debug('%s depth: %d', tag, this.transactionDepth)
    
    const { sqlite3, db } = this.adapter
    
    if (this.transactionDepth === 1) {
      // Commit the main transaction
      try {
        await sqlite3.exec(db, 'COMMIT')
        this.inTransaction = false
        this.transactionDepth = 0
        debug('%s Committed main transaction', tag)
      } catch (error: any) {
        // If commit fails but there's no transaction, that's OK
        if (!error?.message?.includes('no transaction is active')) {
          throw error
        }
        this.inTransaction = false
        this.transactionDepth = 0
      }
    } else if (this.transactionDepth > 1) {
      // Release the savepoint
      const savepointName = `prisma_savepoint_${this.transactionDepth}`
      try {
        await sqlite3.exec(db, `RELEASE SAVEPOINT ${savepointName}`)
        this.transactionDepth--
        debug('%s Released savepoint: %s', tag, savepointName)
      } catch (error: any) {
        // If the savepoint doesn't exist, that might be OK
        console.warn(`Failed to release savepoint ${savepointName}:`, error)
        this.transactionDepth--
      }
    }
  }

  async rollbackTransaction(): Promise<void> {
    const tag = '[js::rollbackTransaction]'
    debug('%s depth: %d', tag, this.transactionDepth)
    
    const { sqlite3, db } = this.adapter
    
    if (this.transactionDepth === 1) {
      // Rollback the main transaction
      try {
        await sqlite3.exec(db, 'ROLLBACK')
        this.inTransaction = false
        this.transactionDepth = 0
        debug('%s Rolled back main transaction', tag)
      } catch (error: any) {
        // If rollback fails but there's no transaction, that's OK
        if (!error?.message?.includes('no transaction is active')) {
          throw error
        }
        this.inTransaction = false
        this.transactionDepth = 0
      }
    } else if (this.transactionDepth > 1) {
      // Rollback to and release the savepoint
      const savepointName = `prisma_savepoint_${this.transactionDepth}`
      try {
        await sqlite3.exec(db, `ROLLBACK TO SAVEPOINT ${savepointName}`)
        await sqlite3.exec(db, `RELEASE SAVEPOINT ${savepointName}`)
        this.transactionDepth--
        debug('%s Rolled back to savepoint: %s', tag, savepointName)
      } catch (error: any) {
        // If the savepoint doesn't exist, that might be OK
        console.warn(`Failed to rollback savepoint ${savepointName}:`, error)
        this.transactionDepth--
      }
    }
  }

  async dispose(): Promise<void> {
    // Ensure any open transaction is rolled back
    if (this.inTransaction) {
      const { sqlite3, db } = this.adapter
      try {
        await sqlite3.exec(db, 'ROLLBACK')
      } catch (error) {
        // Ignore errors during cleanup
      }
      this.inTransaction = false
      this.transactionDepth = 0
    }
    
    await this.release?.()
  }
}

/**
 * wa-sqlite Adapter Factory
 */
export class PrismaWaSQLiteAdapterFactory implements SqlDriverAdapterFactory {
  readonly provider = 'sqlite'
  readonly adapterName = 'prisma-wa-sqlite-adapter'
  private adapterInstance: PrismaWaSQLiteAdapter | null = null

  constructor(private adapter: WaSQLiteAdapter) {}

  async connect(): Promise<SqlDriverAdapter> {
    // Always return the same adapter instance to maintain state
    if (!this.adapterInstance) {
      this.adapterInstance = new PrismaWaSQLiteAdapter(this.adapter, async () => {
        // Cleanup if needed
        const { sqlite3, db } = this.adapter
        await sqlite3.close(db)
        this.adapterInstance = null
      })
    }
    return this.adapterInstance
  }
}

function onError(error: Error): never {
  console.error('Error in performIO: %O', error)
  throw new DriverAdapterError(convertDriverError(error))
}

/**
 * Create wa-sqlite Prisma adapter
 */
export const createWaSQLitePrismaAdapter = async (
  options: {
    logger?: (message: string) => void
  } = {
    logger: undefined
  }
) => {
  options?.logger?.('Initializing wa-sqlite...')

  try {
    // @ts-expect-error
    const { default: SQLiteWasm } = await import('wa-sqlite/dist/wa-sqlite.wasm?url')
    const module = await SQLiteESMFactory({ locateFile: () => SQLiteWasm })
    const sqlite3 = SQLite.Factory(module)

    options?.logger?.('Opening database...')
    // Open database - use in-memory database for browser
    const db = await sqlite3.open_v2(':memory:')

    if (!db || db === 0) {
      throw new Error(`Failed to open database`)
    }

    options?.logger?.('Database opened successfully')

    // Set pragmas for better performance and compatibility
    try {
      await sqlite3.exec(db, 'PRAGMA foreign_keys = ON')
      // WAL mode is not supported in memory databases, but we can try
      try {
        await sqlite3.exec(db, 'PRAGMA journal_mode = WAL')
      } catch (e) {
        // Fallback to default journal mode
        debug('WAL mode not supported, using default journal mode')
      }
      options?.logger?.('Foreign keys enabled')
    } catch (e) {
      console.warn('[wa-sqlite] Failed to set pragmas:', e)
    }

    // Test the connection with a simple query
    try {
      await sqlite3.exec(db, 'SELECT 1')
      options?.logger?.('Database connection verified')
    } catch (e) {
      throw new Error(`Database connection test failed: ${e}`)
    }

    const adapter: WaSQLiteAdapter = {
      sqlite3,
      db,
    }

    return new PrismaWaSQLiteAdapterFactory(adapter)
  } catch (error) {
    options?.logger?.(`Failed to initialize wa-sqlite: ${error}`)
    throw error
  }
}