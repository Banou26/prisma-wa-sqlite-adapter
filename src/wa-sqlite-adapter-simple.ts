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

// Type definitions
interface WaSQLiteAdapter {
  sqlite3: any
  db: number
}

// Helper to clean arguments for SQLite
function cleanArg(arg: unknown): unknown {
  if (arg === undefined || arg === null) return null
  if (arg instanceof Date) return arg.toISOString()
  if (arg instanceof Uint8Array) return Array.from(arg)
  if (typeof arg === 'bigint') return Number(arg)
  return arg
}

// Base queryable class
class WaSQLiteQueryable implements SqlQueryable {
  readonly provider = 'sqlite'
  readonly adapterName = 'prisma-wa-sqlite-adapter'

  constructor(protected readonly adapter: WaSQLiteAdapter) {}

  async queryRaw(query: SqlQuery): Promise<SqlResultSet> {
    debug('[queryRaw] SQL: %s', query.sql)
    debug('[queryRaw] Args: %O', query.args)
    
    const { sqlite3, db } = this.adapter
    const cleanedArgs = query.args.map(cleanArg)

    if (cleanedArgs.length === 0) {
      // No parameters - use exec with callback
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

      return this.createResultSet(columnNames, rows)
    } else {
      // With parameters - use prepared statement
      const str = sqlite3.str_new(db, query.sql)
      const prepared = await sqlite3.prepare_v2(db, sqlite3.str_value(str))
      
      if (!prepared || !prepared.stmt) {
        sqlite3.str_finish(str)
        throw new Error(`Failed to prepare statement: ${query.sql}`)
      }

      const stmt = prepared.stmt

      try {
        // Bind parameters
        cleanedArgs.forEach((arg, index) => {
          const paramIndex = index + 1
          if (arg === null) {
            sqlite3.bind_null(stmt, paramIndex)
          } else if (typeof arg === 'number') {
            if (Number.isInteger(arg)) {
              sqlite3.bind_int(stmt, paramIndex, arg)
            } else {
              sqlite3.bind_double(stmt, paramIndex, arg)
            }
          } else if (typeof arg === 'string') {
            sqlite3.bind_text(stmt, paramIndex, arg)
          } else if (arg instanceof Uint8Array || Array.isArray(arg)) {
            sqlite3.bind_blob(stmt, paramIndex, new Uint8Array(arg))
          } else {
            sqlite3.bind_text(stmt, paramIndex, String(arg))
          }
        })

        // Get column info
        const columnCount = sqlite3.column_count(stmt)
        const columnNames: string[] = []
        for (let i = 0; i < columnCount; i++) {
          columnNames.push(sqlite3.column_name(stmt, i))
        }

        // Fetch rows
        const rows: unknown[][] = []
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
              default:
                row.push(null)
            }
          }
          rows.push(row)
          stepResult = await sqlite3.step(stmt)
        }

        return this.createResultSet(columnNames, rows)
      } finally {
        await sqlite3.finalize(stmt)
        sqlite3.str_finish(str)
      }
    }
  }

  async executeRaw(query: SqlQuery): Promise<number> {
    debug('[executeRaw] SQL: %s', query.sql)
    debug('[executeRaw] Args: %O', query.args)
    
    const { sqlite3, db } = this.adapter
    const cleanedArgs = query.args.map(cleanArg)

    if (cleanedArgs.length === 0) {
      // No parameters
      await sqlite3.exec(db, query.sql)
      return sqlite3.changes(db)
    } else {
      // With parameters - use prepared statement
      const str = sqlite3.str_new(db, query.sql)
      const prepared = await sqlite3.prepare_v2(db, sqlite3.str_value(str))
      
      if (!prepared || !prepared.stmt) {
        sqlite3.str_finish(str)
        throw new Error(`Failed to prepare statement: ${query.sql}`)
      }

      const stmt = prepared.stmt

      try {
        // Bind parameters
        cleanedArgs.forEach((arg, index) => {
          const paramIndex = index + 1
          if (arg === null) {
            sqlite3.bind_null(stmt, paramIndex)
          } else if (typeof arg === 'number') {
            if (Number.isInteger(arg)) {
              sqlite3.bind_int(stmt, paramIndex, arg)
            } else {
              sqlite3.bind_double(stmt, paramIndex, arg)
            }
          } else if (typeof arg === 'string') {
            sqlite3.bind_text(stmt, paramIndex, arg)
          } else if (arg instanceof Uint8Array || Array.isArray(arg)) {
            sqlite3.bind_blob(stmt, paramIndex, new Uint8Array(arg))
          } else {
            sqlite3.bind_text(stmt, paramIndex, String(arg))
          }
        })

        // Execute
        const stepResult = await sqlite3.step(stmt)
        if (stepResult !== SQLite.SQLITE_DONE && stepResult !== SQLite.SQLITE_ROW) {
          throw new Error(`Failed to execute statement. Result code: ${stepResult}`)
        }

        return sqlite3.changes(db)
      } finally {
        await sqlite3.finalize(stmt)
        sqlite3.str_finish(str)
      }
    }
  }

  private createResultSet(columnNames: string[], rows: unknown[][]): SqlResultSet {
    const columnTypes: ColumnTypeEnum[] = []
    
    // Infer column types from first row
    if (rows.length > 0) {
      const firstRow = rows[0]
      firstRow.forEach(value => {
        if (value === null || value === undefined) {
          columnTypes.push(ColumnTypeEnum.Text)
        } else if (typeof value === 'number') {
          columnTypes.push(Number.isInteger(value) ? ColumnTypeEnum.Int32 : ColumnTypeEnum.Double)
        } else if (typeof value === 'string') {
          columnTypes.push(ColumnTypeEnum.Text)
        } else if (typeof value === 'bigint') {
          columnTypes.push(ColumnTypeEnum.Int64)
        } else if (value instanceof Uint8Array || Array.isArray(value)) {
          columnTypes.push(ColumnTypeEnum.Bytes)
        } else {
          columnTypes.push(ColumnTypeEnum.Text)
        }
      })
    } else {
      // No rows - default to Text
      columnNames.forEach(() => columnTypes.push(ColumnTypeEnum.Text))
    }

    return {
      columnNames,
      columnTypes,
      rows,
    }
  }
}

// Transaction implementation
class WaSQLiteTransaction extends WaSQLiteQueryable implements Transaction {
  constructor(
    adapter: WaSQLiteAdapter,
    readonly options: TransactionOptions
  ) {
    super(adapter)
  }

  async commit(): Promise<void> {
    debug('[commit]')
    const { sqlite3, db } = this.adapter
    await sqlite3.exec(db, 'COMMIT')
  }

  async rollback(): Promise<void> {
    debug('[rollback]')
    const { sqlite3, db } = this.adapter
    await sqlite3.exec(db, 'ROLLBACK')
  }
}

// Main adapter
export class PrismaWaSQLiteAdapter extends WaSQLiteQueryable implements SqlDriverAdapter {
  readonly tags = {
    error: '[prisma:error]',
    warn: '[prisma:warn]',
    info: '[prisma:info]',
    query: '[prisma:query]',
  }

  constructor(adapter: WaSQLiteAdapter, private readonly release?: () => Promise<void>) {
    super(adapter)
  }

  async executeScript(script: string): Promise<void> {
    debug('[executeScript] %s...', script.substring(0, 100))
    const { sqlite3, db } = this.adapter
    await sqlite3.exec(db, script)
  }

  getConnectionInfo(): ConnectionInfo {
    return {
      maxBindValues: 32766,
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

    debug('[startTransaction]')
    const { sqlite3, db } = this.adapter
    
    // Start transaction
    await sqlite3.exec(db, 'BEGIN')

    const options: TransactionOptions = {
      usePhantomQuery: false,
    }

    return new WaSQLiteTransaction(this.adapter, options)
  }

  async dispose(): Promise<void> {
    debug('[dispose]')
    await this.release?.()
  }
}

// Factory
export class PrismaWaSQLiteAdapterFactory implements SqlDriverAdapterFactory {
  readonly provider = 'sqlite'
  readonly adapterName = 'prisma-wa-sqlite-adapter'
  private adapterInstance: PrismaWaSQLiteAdapter | null = null

  constructor(private adapter: WaSQLiteAdapter) {}

  async connect(): Promise<SqlDriverAdapter> {
    if (!this.adapterInstance) {
      this.adapterInstance = new PrismaWaSQLiteAdapter(this.adapter, async () => {
        const { sqlite3, db } = this.adapter
        await sqlite3.close(db)
        this.adapterInstance = null
      })
    }
    return this.adapterInstance
  }
}

// Create adapter
export const createWaSQLitePrismaAdapter = async () => {
  // @ts-expect-error
  const { default: SQLiteWasm } = await import('wa-sqlite/dist/wa-sqlite.wasm?url')
  const module = await SQLiteESMFactory({ locateFile: () => SQLiteWasm })
  const sqlite3 = SQLite.Factory(module)

  const db = await sqlite3.open_v2(':memory:')
  if (!db || db === 0) {
    throw new Error('Failed to open database')
  }

  // Enable foreign keys
  await sqlite3.exec(db, 'PRAGMA foreign_keys = ON')

  const adapter: WaSQLiteAdapter = { sqlite3, db }
  return new PrismaWaSQLiteAdapterFactory(adapter)
}