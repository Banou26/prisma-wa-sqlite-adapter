import SQLiteESMFactory from 'wa-sqlite/dist/wa-sqlite.mjs'
import * as SQLite from 'wa-sqlite'

/**
 * better-sqlite3 compatible interface for wa-sqlite
 * This provides a synchronous-looking API that matches better-sqlite3
 * but uses wa-sqlite (WebAssembly SQLite) under the hood
 */

// Type definitions to match better-sqlite3
export interface RunResult {
  changes: number
  lastInsertRowid: number | bigint
}

export interface Statement {
  database: Database
  source: string
  reader: boolean
  readonly: boolean
  busy: boolean
  
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
  
  // Transaction methods
  transaction<T extends (...args: any[]) => any>(fn: T): Transaction<T>
  
  // Pragma methods
  pragma(pragma: string, options?: { simple?: boolean }): any
  
  // Function registration (not implemented)
  function(name: string, fn: (...args: any[]) => any): this
  aggregate(name: string, options: any): this
  
  // Table and backup methods (not implemented)
  table(name: string, options: any): this
  backup(destination: string, options?: any): Promise<any>
  
  // Serialization
  serialize(options?: { attached?: string }): Buffer
  
  // Event handlers
  on(event: string, listener: (...args: any[]) => void): this
  off(event: string, listener: (...args: any[]) => void): this
  
  // Other methods
  loadExtension(path: string): this
  defaultSafeIntegers(toggleState?: boolean): this
  unsafeMode(toggleState?: boolean): this
}

class WaSQLiteStatement implements Statement {
  database: Database
  source: string
  reader: boolean = false
  readonly: boolean = false
  busy: boolean = false
  
  private sqlite3: any
  private db: number
  private stmt: any = null
  private str: number | null = null
  private params: any[] = []
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
  }
  
  private async prepareStatement() {
    if (!this.stmt) {
      this.str = this.sqlite3.str_new(this.db, this.source)
      const prepared = await this.sqlite3.prepare_v2(this.db, this.sqlite3.str_value(this.str))
      if (!prepared || !prepared.stmt) {
        throw new Error(`Failed to prepare statement: ${this.source}`)
      }
      this.stmt = prepared.stmt
    }
  }
  
  private async bindParameters(params: any[]) {
    const bindParams = params.length > 0 ? params : this.params
    
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
        // Convert bigint to number for wa-sqlite
        this.sqlite3.bind_int(this.stmt, index, Number(param))
      } else if (typeof param === 'string') {
        this.sqlite3.bind_text(this.stmt, index, param)
      } else if (param instanceof Buffer || param instanceof Uint8Array) {
        this.sqlite3.bind_blob(this.stmt, index, param)
      } else if (Array.isArray(param)) {
        this.sqlite3.bind_blob(this.stmt, index, new Uint8Array(param))
      } else {
        // Convert to string as fallback
        this.sqlite3.bind_text(this.stmt, index, String(param))
      }
    }
  }
  
  private async finalize() {
    if (this.stmt) {
      await this.sqlite3.finalize(this.stmt)
      this.stmt = null
    }
    if (this.str !== null) {
      this.sqlite3.str_finish(this.str)
      this.str = null
    }
  }
  
  private convertValue(value: any, type: number): any {
    if (value === null || value === undefined) {
      return null
    }
    
    if (this.safeIntegersMode && type === SQLite.SQLITE_INTEGER) {
      const num = this.sqlite3.column_int(this.stmt, i)
      // Convert large integers to BigInt if needed
      if (num > Number.MAX_SAFE_INTEGER || num < Number.MIN_SAFE_INTEGER) {
        return BigInt(num)
      }
    }
    
    return value
  }
  
  run(...params: any[]): RunResult {
    // Run synchronously (blocking) - wa-sqlite operations are async but we'll make them sync-like
    const asyncRun = async () => {
      try {
        await this.prepareStatement()
        await this.bindParameters(params)
        
        const stepResult = await this.sqlite3.step(this.stmt)
        if (stepResult !== SQLite.SQLITE_DONE && stepResult !== SQLite.SQLITE_ROW) {
          throw new Error(`Failed to execute statement. Result code: ${stepResult}`)
        }
        
        const changes = this.sqlite3.changes(this.db)
        const lastInsertRowid = this.sqlite3.last_insert_rowid(this.db)
        
        // Reset for next execution
        await this.sqlite3.reset(this.stmt)
        
        return {
          changes,
          lastInsertRowid
        }
      } catch (error) {
        await this.finalize()
        throw error
      }
    }
    
    // Block until complete (not truly synchronous but appears so)
    let result: RunResult = { changes: 0, lastInsertRowid: 0 }
    let error: any = null
    let done = false
    
    asyncRun().then(r => {
      result = r
      done = true
    }).catch(e => {
      error = e
      done = true
    })
    
    // Busy wait (not ideal but maintains sync interface)
    while (!done) {
      // This is a hack to make async appear sync
      // In a real implementation, we'd need true synchronous SQLite
    }
    
    if (error) throw error
    return result
  }
  
  get(...params: any[]): any {
    const asyncGet = async () => {
      try {
        await this.prepareStatement()
        await this.bindParameters(params)
        
        const stepResult = await this.sqlite3.step(this.stmt)
        
        if (stepResult === SQLite.SQLITE_ROW) {
          const columnCount = this.sqlite3.column_count(this.stmt)
          
          if (this.pluckMode) {
            // Return only the first column value
            const type = this.sqlite3.column_type(this.stmt, 0)
            return this.getColumnValue(0, type)
          } else if (this.rawMode) {
            // Return array of values
            const row: any[] = []
            for (let i = 0; i < columnCount; i++) {
              const type = this.sqlite3.column_type(this.stmt, i)
              row.push(this.getColumnValue(i, type))
            }
            return row
          } else {
            // Return object with column names as keys
            const row: any = {}
            for (let i = 0; i < columnCount; i++) {
              const name = this.sqlite3.column_name(this.stmt, i)
              const type = this.sqlite3.column_type(this.stmt, i)
              row[name] = this.getColumnValue(i, type)
            }
            return row
          }
        }
        
        // Reset for next execution
        await this.sqlite3.reset(this.stmt)
        return undefined
      } catch (error) {
        await this.finalize()
        throw error
      }
    }
    
    // Block until complete
    let result: any = undefined
    let error: any = null
    let done = false
    
    asyncGet().then(r => {
      result = r
      done = true
    }).catch(e => {
      error = e
      done = true
    })
    
    while (!done) {
      // Busy wait
    }
    
    if (error) throw error
    return result
  }
  
  all(...params: any[]): any[] {
    const asyncAll = async () => {
      try {
        await this.prepareStatement()
        await this.bindParameters(params)
        
        const rows: any[] = []
        const columnCount = this.sqlite3.column_count(this.stmt)
        
        let stepResult = await this.sqlite3.step(this.stmt)
        while (stepResult === SQLite.SQLITE_ROW) {
          if (this.pluckMode) {
            // Return only the first column value
            const type = this.sqlite3.column_type(this.stmt, 0)
            rows.push(this.getColumnValue(0, type))
          } else if (this.rawMode) {
            // Return array of values
            const row: any[] = []
            for (let i = 0; i < columnCount; i++) {
              const type = this.sqlite3.column_type(this.stmt, i)
              row.push(this.getColumnValue(i, type))
            }
            rows.push(row)
          } else {
            // Return object with column names as keys
            const row: any = {}
            for (let i = 0; i < columnCount; i++) {
              const name = this.sqlite3.column_name(this.stmt, i)
              const type = this.sqlite3.column_type(this.stmt, i)
              row[name] = this.getColumnValue(i, type)
            }
            rows.push(row)
          }
          
          stepResult = await this.sqlite3.step(this.stmt)
        }
        
        // Reset for next execution
        await this.sqlite3.reset(this.stmt)
        return rows
      } catch (error) {
        await this.finalize()
        throw error
      }
    }
    
    // Block until complete
    let result: any[] = []
    let error: any = null
    let done = false
    
    asyncAll().then(r => {
      result = r
      done = true
    }).catch(e => {
      error = e
      done = true
    })
    
    while (!done) {
      // Busy wait
    }
    
    if (error) throw error
    return result
  }
  
  private getColumnValue(index: number, type: number): any {
    switch (type) {
      case SQLite.SQLITE_INTEGER:
        return this.sqlite3.column_int(this.stmt, index)
      case SQLite.SQLITE_FLOAT:
        return this.sqlite3.column_double(this.stmt, index)
      case SQLite.SQLITE_TEXT:
        return this.sqlite3.column_text(this.stmt, index)
      case SQLite.SQLITE_BLOB:
        return this.sqlite3.column_blob(this.stmt, index)
      case SQLite.SQLITE_NULL:
      default:
        return null
    }
  }
  
  iterate(...params: any[]): IterableIterator<any> {
    // This would need async generators which don't work well with sync interface
    // Return all rows as an iterator instead
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
    // This would need to prepare the statement first
    // For now, return empty array
    return []
  }
  
  bind(...params: any[]): this {
    this.params = params
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
  
  constructor(sqlite3: any, db: number) {
    this.sqlite3 = sqlite3
    this.db = db
  }
  
  prepare(sql: string): Statement {
    // Create a new statement each time for simplicity
    // In production, you might want to cache prepared statements
    return new WaSQLiteStatement(this, this.sqlite3, this.db, sql)
  }
  
  exec(sql: string): this {
    const asyncExec = async () => {
      await this.sqlite3.exec(this.db, sql)
    }
    
    // Block until complete
    let error: any = null
    let done = false
    
    asyncExec().then(() => {
      done = true
    }).catch(e => {
      error = e
      done = true
    })
    
    while (!done) {
      // Busy wait
    }
    
    if (error) throw error
    return this
  }
  
  close(): this {
    const asyncClose = async () => {
      await this.sqlite3.close(this.db)
    }
    
    // Block until complete
    let error: any = null
    let done = false
    
    asyncClose().then(() => {
      done = true
    }).catch(e => {
      error = e
      done = true
    })
    
    while (!done) {
      // Busy wait
    }
    
    if (error) throw error
    this.open = false
    return this
  }
  
  transaction<T extends (...args: any[]) => any>(fn: T): Transaction<T> {
    const self = this
    
    const runTransaction = (...args: Parameters<T>): ReturnType<T> => {
      self.exec('BEGIN')
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
    
    // Add transaction mode methods
    const transaction = runTransaction as Transaction<T>
    transaction.default = runTransaction
    transaction.deferred = runTransaction
    transaction.immediate = (...args: Parameters<T>): ReturnType<T> => {
      self.exec('BEGIN IMMEDIATE')
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
    transaction.exclusive = (...args: Parameters<T>): ReturnType<T> => {
      self.exec('BEGIN EXCLUSIVE')
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
    
    return transaction
  }
  
  pragma(pragma: string, options?: { simple?: boolean }): any {
    const stmt = this.prepare(`PRAGMA ${pragma}`)
    const result = stmt.all()
    
    if (options?.simple) {
      // Return single value for simple mode
      if (result.length > 0 && typeof result[0] === 'object') {
        const keys = Object.keys(result[0])
        if (keys.length > 0) {
          return result[0][keys[0]]
        }
      }
    }
    
    return result
  }
  
  function(name: string, fn: (...args: any[]) => any): this {
    // Not implemented - would need SQLite function registration
    console.warn('User-defined functions not yet implemented in wa-sqlite adapter')
    return this
  }
  
  aggregate(name: string, options: any): this {
    // Not implemented
    console.warn('Aggregate functions not yet implemented in wa-sqlite adapter')
    return this
  }
  
  table(name: string, options: any): this {
    // Not implemented
    console.warn('Virtual tables not yet implemented in wa-sqlite adapter')
    return this
  }
  
  backup(destination: string, options?: any): Promise<any> {
    // Not implemented
    throw new Error('Backup not yet implemented in wa-sqlite adapter')
  }
  
  serialize(options?: { attached?: string }): Buffer {
    // Not implemented - would need to export database to buffer
    throw new Error('Serialize not yet implemented in wa-sqlite adapter')
  }
  
  on(event: string, listener: (...args: any[]) => void): this {
    // Not implemented - would need event emitter
    return this
  }
  
  off(event: string, listener: (...args: any[]) => void): this {
    // Not implemented - would need event emitter
    return this
  }
  
  loadExtension(path: string): this {
    // Not implemented - wa-sqlite doesn't support extensions
    console.warn('Extensions not supported in wa-sqlite adapter')
    return this
  }
  
  defaultSafeIntegers(toggleState?: boolean): this {
    // Not implemented - would need global state
    return this
  }
  
  unsafeMode(toggleState?: boolean): this {
    // Not implemented
    return this
  }
}

/**
 * Create a better-sqlite3 compatible database using wa-sqlite
 */
export async function createBetterSqlite3Compatible(filename?: string): Promise<Database> {
  // @ts-expect-error
  const { default: SQLiteWasm } = await import('wa-sqlite/dist/wa-sqlite.wasm?url')
  const module = await SQLiteESMFactory({ locateFile: () => SQLiteWasm })
  const sqlite3 = SQLite.Factory(module)
  
  // Open database - always use in-memory for wa-sqlite in browser
  const db = await sqlite3.open_v2(':memory:')
  
  if (!db || db === 0) {
    throw new Error(`Failed to open database`)
  }
  
  // Enable foreign keys
  await sqlite3.exec(db, 'PRAGMA foreign_keys = ON')
  
  return new WaSQLiteDatabase(sqlite3, db)
}