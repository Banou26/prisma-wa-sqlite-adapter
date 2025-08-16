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
import type { Database } from './wa-sqlite-better-sqlite3'

const debug = Debug('prisma:driver-adapter:wa-sqlite')

// Convert better-sqlite3 row to Prisma result
function convertRow(row: any, columnNames: string[]): any[] {
  if (Array.isArray(row)) {
    return row
  }
  
  // Object form - convert to array based on column order
  const result: any[] = []
  for (const name of columnNames) {
    result.push(row[name])
  }
  return result
}

// Infer column types from values
function inferColumnTypes(rows: any[], columnNames: string[]): ColumnTypeEnum[] {
  const columnTypes: ColumnTypeEnum[] = []
  
  if (rows.length === 0) {
    // Default to Text when no rows
    columnNames.forEach(() => columnTypes.push(ColumnTypeEnum.Text))
    return columnTypes
  }
  
  const firstRow = rows[0]
  const values = Array.isArray(firstRow) ? firstRow : columnNames.map(name => firstRow[name])
  
  values.forEach(value => {
    if (value === null || value === undefined) {
      columnTypes.push(ColumnTypeEnum.Text)
    } else if (typeof value === 'number') {
      columnTypes.push(Number.isInteger(value) ? ColumnTypeEnum.Int32 : ColumnTypeEnum.Double)
    } else if (typeof value === 'string') {
      columnTypes.push(ColumnTypeEnum.Text)
    } else if (typeof value === 'bigint') {
      columnTypes.push(ColumnTypeEnum.Int64)
    } else if (value instanceof Buffer || value instanceof Uint8Array) {
      columnTypes.push(ColumnTypeEnum.Bytes)
    } else {
      columnTypes.push(ColumnTypeEnum.Text)
    }
  })
  
  return columnTypes
}

// Base queryable implementation
class WaSQLiteQueryable implements SqlQueryable {
  readonly provider = 'sqlite'
  readonly adapterName = 'prisma-wa-sqlite-adapter'
  
  constructor(protected readonly db: Database) {}
  
  async queryRaw(query: SqlQuery): Promise<SqlResultSet> {
    debug('[queryRaw] SQL: %s', query.sql)
    debug('[queryRaw] Args: %O', query.args)
    
    try {
      const stmt = this.db.prepare(query.sql)
      
      // Get column info
      const columns = stmt.columns()
      const columnNames = columns.map(c => c.name)
      
      // Execute query
      const rows = stmt.all(...query.args)
      
      // Convert to Prisma format
      const columnTypes = inferColumnTypes(rows, columnNames)
      const convertedRows = rows.map(row => convertRow(row, columnNames))
      
      return {
        columnNames,
        columnTypes,
        rows: convertedRows,
      }
    } catch (error) {
      this.handleError(error)
    }
  }
  
  async executeRaw(query: SqlQuery): Promise<number> {
    debug('[executeRaw] SQL: %s', query.sql)
    debug('[executeRaw] Args: %O', query.args)
    
    try {
      const stmt = this.db.prepare(query.sql)
      const result = stmt.run(...query.args)
      return result.changes
    } catch (error) {
      this.handleError(error)
    }
  }
  
  protected handleError(error: any): never {
    const message = error?.message || String(error)
    
    // Map SQLite errors to Prisma error types
    if (message.includes('UNIQUE constraint failed')) {
      throw new DriverAdapterError({
        kind: 'UniqueConstraintViolation',
        message,
      })
    }
    
    if (message.includes('FOREIGN KEY constraint failed')) {
      throw new DriverAdapterError({
        kind: 'ForeignKeyConstraintViolation',
        message,
      })
    }
    
    if (message.includes('database is locked')) {
      throw new DriverAdapterError({
        kind: 'DatabaseTimeout',
        message,
      })
    }
    
    throw new DriverAdapterError({
      kind: 'GenericDatabaseError',
      message,
    })
  }
}

// Transaction implementation
class WaSQLiteTransaction extends WaSQLiteQueryable implements Transaction {
  constructor(
    db: Database,
    readonly options: TransactionOptions,
    private readonly release: () => void
  ) {
    super(db)
  }
  
  async commit(): Promise<void> {
    debug('[commit]')
    try {
      this.db.exec('COMMIT')
    } finally {
      this.release()
    }
  }
  
  async rollback(): Promise<void> {
    debug('[rollback]')
    try {
      this.db.exec('ROLLBACK')
    } finally {
      this.release()
    }
  }
}

// Simple mutex for transaction serialization
class Mutex {
  private queue: (() => void)[] = []
  private locked = false
  
  async acquire(): Promise<() => void> {
    return new Promise(resolve => {
      const unlock = () => {
        if (this.queue.length > 0) {
          const next = this.queue.shift()!
          next()
        } else {
          this.locked = false
        }
      }
      
      if (!this.locked) {
        this.locked = true
        resolve(unlock)
      } else {
        this.queue.push(() => resolve(unlock))
      }
    })
  }
}

// Main adapter
class WaSQLiteAdapter extends WaSQLiteQueryable implements SqlDriverAdapter {
  readonly tags = {
    error: '[prisma:error]',
    warn: '[prisma:warn]',
    info: '[prisma:info]',
    query: '[prisma:query]',
  }
  
  private mutex = new Mutex()
  
  constructor(db: Database) {
    super(db)
  }
  
  async executeScript(script: string): Promise<void> {
    debug('[executeScript] %s...', script.substring(0, 100))
    try {
      this.db.exec(script)
    } catch (error) {
      this.handleError(error)
    }
  }
  
  getConnectionInfo(): ConnectionInfo {
    return {
      maxBindValues: 999, // SQLite default
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
    
    const options: TransactionOptions = {
      usePhantomQuery: false,
    }
    
    try {
      const release = await this.mutex.acquire()
      this.db.exec('BEGIN')
      return new WaSQLiteTransaction(this.db, options, release)
    } catch (error) {
      this.handleError(error)
    }
  }
  
  async dispose(): Promise<void> {
    debug('[dispose]')
    this.db.close()
  }
}

// Factory implementation
class WaSQLiteAdapterFactory implements SqlDriverAdapterFactory {
  readonly provider = 'sqlite'
  readonly adapterName = 'prisma-wa-sqlite-adapter'
  private adapter: WaSQLiteAdapter | null = null
  
  constructor(private db: Database) {}
  
  async connect(): Promise<SqlDriverAdapter> {
    if (!this.adapter) {
      this.adapter = new WaSQLiteAdapter(this.db)
    }
    return this.adapter
  }
}

// Factory function
export function createWaSQLiteAdapterFactory(db: Database): SqlDriverAdapterFactory {
  return new WaSQLiteAdapterFactory(db)
}