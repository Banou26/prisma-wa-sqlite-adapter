import SQLiteESMFactory from 'wa-sqlite/dist/wa-sqlite.mjs'
import * as SQLite from 'wa-sqlite'

/**
 * better-sqlite3 compatible interface for wa-sqlite
 * Provides the same synchronous API as better-sqlite3 using wa-sqlite
 */

// Type definitions matching better-sqlite3
export interface RunResult {
  changes: number
  lastInsertRowid: number | bigint
}

export interface Options {
  readonly?: boolean
  fileMustExist?: boolean
  timeout?: number
  verbose?: ((message?: any, ...additionalArgs: any[]) => void) | undefined
}

export interface Statement {
  database: Database
  source: string
  reader: boolean
  readonly: boolean
  
  run(...params: any[]): RunResult
  get(...params: any[]): any
  all(...params: any[]): any[]
  iterate(...params: any[]): IterableIterator<any>
  pluck(toggleState?: boolean): this
  expand(toggleState?: boolean): this
  raw(toggleState?: boolean): this
  columns(): Array<{ name: string; column: string | null; table: string | null; database: string | null; type: string | null }>
  bind(...params: any[]): this
  safeIntegers(toggleState?: boolean): this
}

export interface Transaction<T extends (...args: any[]) => any> {
  (...args: Parameters<T>): ReturnType<T>
  default(...args: Parameters<T>): ReturnType<T>
  deferred(...args: Parameters<T>): ReturnType<T>
  immediate(...args: Parameters<T>): ReturnType<T>
  exclusive(...args: Parameters<T>): ReturnType<T>
}

export interface Database {
  memory: boolean
  readonly: boolean
  name: string
  open: boolean
  inTransaction: boolean
  
  prepare(sql: string): Statement
  exec(sql: string): this
  close(): this
  
  transaction<T extends (...args: any[]) => any>(fn: T): Transaction<T>
  pragma(pragma: string, options?: { simple?: boolean }): any
  
  function(name: string, fn: (...args: any[]) => any): this
  aggregate(name: string, options: any): this
  table(name: string, options: any): this
  loadExtension(path: string): this
  defaultSafeIntegers(toggleState?: boolean): this
  unsafeMode(toggleState?: boolean): this
  
  backup(destination: string, options?: any): Promise<any>
  serialize(options?: { attached?: string }): Buffer
}

class WaSQLiteStatement implements Statement {
  database: Database
  source: string
  reader: boolean = false
  readonly: boolean = false
  
  private sqlite3: any
  private db: number
  private stmt: any = null
  private str: number | null = null
  private boundParams: any[] = []
  private pluckMode = false
  private expandMode = false
  private rawMode = false
  private safeIntegersMode = false
  
  constructor(database: Database, sqlite3: any, db: number, sql: string) {
    this.database = database
    this.sqlite3 = sqlite3
    this.db = db
    this.source = sql
    
    // Determine if this is a reader query
    const trimmedSql = sql.trim().toUpperCase()
    this.reader = trimmedSql.startsWith('SELECT') || 
                  trimmedSql.startsWith('WITH') ||
                  trimmedSql.startsWith('VALUES') ||
                  trimmedSql.startsWith('PRAGMA')
    this.readonly = this.reader
    
    // Prepare the statement immediately
    this.prepareStatement()
  }
  
  private prepareStatement() {
    if (!this.stmt) {
      this.str = this.sqlite3.str_new(this.db, this.source)
      const prepared = this.sqlite3.prepare_v2(this.db, this.sqlite3.str_value(this.str))
      if (!prepared || !prepared.stmt) {
        if (this.str !== null) {
          this.sqlite3.str_finish(this.str)
        }
        throw new Error(`Failed to prepare statement: ${this.source}`)
      }
      this.stmt = prepared.stmt
    }
  }
  
  private bindParameters(params: any[]) {
    const bindParams = params.length > 0 ? params : this.boundParams
    
    // Reset bindings
    this.sqlite3.reset(this.stmt)
    this.sqlite3.clear_bindings(this.stmt)
    
    for (let i = 0; i < bindParams.length; i++) {
      const param = bindParams[i]
      const index = i + 1 // SQLite uses 1-based indexing
      
      if (param === null || param === undefined) {
        this.sqlite3.bind_null(this.stmt, index)
      } else if (typeof param === 'number') {
        if (Number.isInteger(param)) {
          this.sqlite3.bind_int(this.stmt, index, param)
        } else {
          this.sqlite3.bind_double(this.stmt, index, param)
        }
      } else if (typeof param === 'bigint') {
        // wa-sqlite doesn't have bind_int64, convert to number
        this.sqlite3.bind_int(this.stmt, index, Number(param))
      } else if (typeof param === 'string') {
        this.sqlite3.bind_text(this.stmt, index, param)
      } else if (param instanceof Buffer) {
        this.sqlite3.bind_blob(this.stmt, index, new Uint8Array(param))
      } else if (param instanceof Uint8Array) {
        this.sqlite3.bind_blob(this.stmt, index, param)
      } else if (Array.isArray(param)) {
        this.sqlite3.bind_blob(this.stmt, index, new Uint8Array(param))
      } else {
        // Convert to string as fallback
        this.sqlite3.bind_text(this.stmt, index, String(param))
      }
    }
  }
  
  finalize() {
    if (this.stmt) {
      this.sqlite3.finalize(this.stmt)
      this.stmt = null
    }
    if (this.str !== null) {
      this.sqlite3.str_finish(this.str)
      this.str = null
    }
  }
  
  run(...params: any[]): RunResult {
    this.bindParameters(params)
    
    const stepResult = this.sqlite3.step(this.stmt)
    if (stepResult !== SQLite.SQLITE_DONE && stepResult !== SQLite.SQLITE_ROW) {
      const error = new Error(`Failed to execute statement: ${this.sqlite3.errmsg(this.db)}`)
      throw error
    }
    
    const changes = this.sqlite3.changes(this.db)
    const lastInsertRowid = this.sqlite3.last_insert_rowid(this.db)
    
    // Reset for next execution
    this.sqlite3.reset(this.stmt)
    
    return {
      changes,
      lastInsertRowid
    }
  }
  
  get(...params: any[]): any {
    this.bindParameters(params)
    
    const stepResult = this.sqlite3.step(this.stmt)
    
    if (stepResult === SQLite.SQLITE_ROW) {
      const columnCount = this.sqlite3.column_count(this.stmt)
      
      let result: any
      
      if (this.pluckMode) {
        // Return only the first column value
        result = this.getColumnValue(0)
      } else if (this.rawMode) {
        // Return array of values
        result = []
        for (let i = 0; i < columnCount; i++) {
          result.push(this.getColumnValue(i))
        }
      } else {
        // Return object with column names as keys
        result = {}
        for (let i = 0; i < columnCount; i++) {
          const name = this.sqlite3.column_name(this.stmt, i)
          result[name] = this.getColumnValue(i)
        }
      }
      
      // Reset for next execution
      this.sqlite3.reset(this.stmt)
      return result
    }
    
    // Reset for next execution
    this.sqlite3.reset(this.stmt)
    return undefined
  }
  
  all(...params: any[]): any[] {
    this.bindParameters(params)
    
    const rows: any[] = []
    const columnCount = this.sqlite3.column_count(this.stmt)
    
    let stepResult = this.sqlite3.step(this.stmt)
    while (stepResult === SQLite.SQLITE_ROW) {
      let row: any
      
      if (this.pluckMode) {
        // Return only the first column value
        row = this.getColumnValue(0)
      } else if (this.rawMode) {
        // Return array of values
        row = []
        for (let i = 0; i < columnCount; i++) {
          row.push(this.getColumnValue(i))
        }
      } else {
        // Return object with column names as keys
        row = {}
        for (let i = 0; i < columnCount; i++) {
          const name = this.sqlite3.column_name(this.stmt, i)
          row[name] = this.getColumnValue(i)
        }
      }
      
      rows.push(row)
      stepResult = this.sqlite3.step(this.stmt)
    }
    
    // Reset for next execution
    this.sqlite3.reset(this.stmt)
    return rows
  }
  
  private getColumnValue(index: number): any {
    const type = this.sqlite3.column_type(this.stmt, index)
    
    switch (type) {
      case SQLite.SQLITE_INTEGER:
        const value = this.sqlite3.column_int(this.stmt, index)
        // Convert to BigInt if safeIntegers mode is on and value is large
        if (this.safeIntegersMode && 
            (value > Number.MAX_SAFE_INTEGER || value < Number.MIN_SAFE_INTEGER)) {
          return BigInt(value)
        }
        return value
      case SQLite.SQLITE_FLOAT:
        return this.sqlite3.column_double(this.stmt, index)
      case SQLite.SQLITE_TEXT:
        return this.sqlite3.column_text(this.stmt, index)
      case SQLite.SQLITE_BLOB:
        const blob = this.sqlite3.column_blob(this.stmt, index)
        // Convert to Buffer for better-sqlite3 compatibility
        return Buffer.from(blob)
      case SQLite.SQLITE_NULL:
      default:
        return null
    }
  }
  
  iterate(...params: any[]): IterableIterator<any> {
    const rows = this.all(...params)
    return rows[Symbol.iterator]()
  }
  
  pluck(toggleState?: boolean): this {
    this.pluckMode = toggleState !== false
    return this
  }
  
  expand(toggleState?: boolean): this {
    this.expandMode = toggleState !== false
    return this
  }
  
  raw(toggleState?: boolean): this {
    this.rawMode = toggleState !== false
    return this
  }
  
  columns(): Array<{ name: string; column: string | null; table: string | null; database: string | null; type: string | null }> {
    const columnCount = this.sqlite3.column_count(this.stmt)
    const columns = []
    
    for (let i = 0; i < columnCount; i++) {
      columns.push({
        name: this.sqlite3.column_name(this.stmt, i),
        column: this.sqlite3.column_name(this.stmt, i),
        table: null, // wa-sqlite doesn't provide table info easily
        database: null,
        type: null
      })
    }
    
    return columns
  }
  
  bind(...params: any[]): this {
    this.boundParams = params
    return this
  }
  
  safeIntegers(toggleState?: boolean): this {
    this.safeIntegersMode = toggleState !== false
    return this
  }
}

export class WaSQLiteDatabase implements Database {
  memory: boolean = true
  readonly: boolean = false
  name: string = ':memory:'
  open: boolean = true
  inTransaction: boolean = false
  
  private sqlite3: any
  private db: number
  private statements: Map<string, WaSQLiteStatement> = new Map()
  private defaultSafeIntegersMode = false
  
  constructor(sqlite3: any, db: number, name: string = ':memory:') {
    this.sqlite3 = sqlite3
    this.db = db
    this.name = name
    this.memory = name === ':memory:'
  }
  
  prepare(sql: string): Statement {
    // Create a new statement
    const stmt = new WaSQLiteStatement(this, this.sqlite3, this.db, sql)
    if (this.defaultSafeIntegersMode) {
      stmt.safeIntegers(true)
    }
    return stmt
  }
  
  exec(sql: string): this {
    this.sqlite3.exec(this.db, sql)
    return this
  }
  
  close(): this {
    // Close all prepared statements first
    for (const stmt of this.statements.values()) {
      stmt.finalize()
    }
    this.statements.clear()
    
    this.sqlite3.close(this.db)
    this.open = false
    return this
  }
  
  transaction<T extends (...args: any[]) => any>(fn: T): Transaction<T> {
    const self = this
    
    const runTransaction = (mode: string = 'DEFERRED') => {
      return (...args: Parameters<T>): ReturnType<T> => {
        self.exec(`BEGIN ${mode}`)
        self.inTransaction = true
        
        try {
          const result = fn(...args)
          self.exec('COMMIT')
          self.inTransaction = false
          return result
        } catch (error) {
          self.exec('ROLLBACK')
          self.inTransaction = false
          throw error
        }
      }
    }
    
    // Create transaction function with mode methods
    const transaction = runTransaction() as Transaction<T>
    transaction.default = runTransaction()
    transaction.deferred = runTransaction('DEFERRED')
    transaction.immediate = runTransaction('IMMEDIATE')
    transaction.exclusive = runTransaction('EXCLUSIVE')
    
    return transaction
  }
  
  pragma(pragma: string, options?: { simple?: boolean }): any {
    const stmt = this.prepare(`PRAGMA ${pragma}`)
    const result = stmt.all()
    
    if (options?.simple && result.length > 0) {
      // Return single value for simple mode
      if (typeof result[0] === 'object' && result[0] !== null) {
        const keys = Object.keys(result[0])
        if (keys.length > 0) {
          return result[0][keys[0]]
        }
      }
      return result[0]
    }
    
    return result
  }
  
  function(name: string, fn: (...args: any[]) => any): this {
    // wa-sqlite doesn't support custom functions easily
    console.warn('User-defined functions not yet implemented in wa-sqlite adapter')
    return this
  }
  
  aggregate(name: string, options: any): this {
    console.warn('Aggregate functions not yet implemented in wa-sqlite adapter')
    return this
  }
  
  table(name: string, options: any): this {
    console.warn('Virtual tables not yet implemented in wa-sqlite adapter')
    return this
  }
  
  loadExtension(path: string): this {
    console.warn('Extensions not supported in wa-sqlite adapter')
    return this
  }
  
  defaultSafeIntegers(toggleState?: boolean): this {
    this.defaultSafeIntegersMode = toggleState !== false
    return this
  }
  
  unsafeMode(toggleState?: boolean): this {
    // Not applicable for wa-sqlite
    return this
  }
  
  backup(destination: string, options?: any): Promise<any> {
    throw new Error('Backup not yet implemented in wa-sqlite adapter')
  }
  
  serialize(options?: { attached?: string }): Buffer {
    throw new Error('Serialize not yet implemented in wa-sqlite adapter')
  }
}

// Global state for wa-sqlite initialization
let wasmModule: any = null
let sqlite3Api: any = null

/**
 * Initialize wa-sqlite (should be called once)
 */
async function initWaSQLite() {
  if (!sqlite3Api) {
    // @ts-expect-error
    const { default: SQLiteWasm } = await import('wa-sqlite/dist/wa-sqlite.wasm?url')
    wasmModule = await SQLiteESMFactory({ locateFile: () => SQLiteWasm })
    sqlite3Api = SQLite.Factory(wasmModule)
  }
  return sqlite3Api
}

/**
 * Create a better-sqlite3 compatible database using wa-sqlite
 * This mimics the better-sqlite3 constructor
 */
export default async function Database(filename?: string, options?: Options): Promise<WaSQLiteDatabase> {
  const sqlite3 = await initWaSQLite()
  
  // wa-sqlite in browser always uses in-memory database
  const dbName = ':memory:'
  const db = sqlite3.open_v2(dbName)
  
  if (!db || db === 0) {
    throw new Error('Failed to open database')
  }
  
  // Set default pragmas like better-sqlite3
  sqlite3.exec(db, 'PRAGMA foreign_keys = ON')
  
  // Try to set journal mode to WAL (may not work for in-memory)
  try {
    sqlite3.exec(db, 'PRAGMA journal_mode = WAL')
  } catch (e) {
    // Ignore - WAL not supported for in-memory databases
  }
  
  const database = new WaSQLiteDatabase(sqlite3, db, dbName)
  
  // Set readonly if specified
  if (options?.readonly) {
    database.readonly = true
  }
  
  return database
}

// Export the Database constructor as both default and named export
export { Database }