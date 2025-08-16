import type {
  SqlDriverAdapter,
  SqlDriverAdapterFactory,
  SqlMigrationAwareDriverAdapterFactory,
  SqlQuery,
  SqlResultSet,
  Transaction,
  IsolationLevel,
  TransactionOptions,
  ConnectionInfo,
  ColumnType,
  ArgType
} from '@prisma/driver-adapter-utils'
import { DriverAdapterError, ColumnTypeEnum, Debug } from '@prisma/driver-adapter-utils'

const debug = Debug('prisma:driver-adapter:wa-sqlite')

// Import wa-sqlite types - these are the actual types from wa-sqlite
interface SQLiteAPI {
  open_v2(filename: string, flags?: number, vfs?: string): Promise<number>
  close(db: number): Promise<number>
  prepare_v2(db: number, sql: number): Promise<{ stmt: number, sql: number } | null>
  bind_text(stmt: number, index: number, value: string): number
  bind_int(stmt: number, index: number, value: number): number
  bind_int64(stmt: number, index: number, value: bigint): number
  bind_double(stmt: number, index: number, value: number): number
  bind_blob(stmt: number, index: number, value: Uint8Array | number[]): number
  bind_null(stmt: number, index: number): number
  bind(stmt: number, index: number, value: any): number
  step(stmt: number): Promise<number>
  finalize(stmt: number): Promise<number>
  column_count(stmt: number): number
  column_name(stmt: number, index: number): string
  column_type(stmt: number, index: number): number
  column_text(stmt: number, index: number): string
  column_int(stmt: number, index: number): number
  column_int64(stmt: number, index: number): bigint
  column_double(stmt: number, index: number): number
  column_blob(stmt: number, index: number): Uint8Array
  reset(stmt: number): Promise<number>
  changes(db: number): number
  exec(db: number, sql: string, callback?: (row: any[], columns: string[]) => void): Promise<number>
  str_new(db: number, s?: string): number
  str_value(str: number): number
  str_finish(str: number): void
  row(stmt: number): any[]
  get_autocommit?(db: number): number
}

interface WaSQLiteConfig {
  url?: string
  sqlite3?: SQLiteAPI
}

class WaSQLiteQueryable {
  protected db: number
  protected sqlite3: SQLiteAPI

  constructor(sqlite3: SQLiteAPI, db: number) {
    this.sqlite3 = sqlite3
    this.db = db
  }

  provider = 'sqlite' as const
  adapterName = '@banou/prisma-wa-sqlite-adapter'

  async queryRaw(query: SqlQuery): Promise<SqlResultSet> {
    const tag = '[wa-sqlite::queryRaw]'
    debug(`${tag} %O`, query)

    // Create a string object for the SQL
    const str = this.sqlite3.str_new(this.db, query.sql)
    let prepared: { stmt: number, sql: number } | null = null
    
    try {
      // Prepare the statement
      prepared = await this.sqlite3.prepare_v2(this.db, this.sqlite3.str_value(str))
      if (!prepared) {
        throw new Error('Failed to prepare statement')
      }

      // Bind parameters using the convenient bind method
      for (let i = 0; i < query.args.length; i++) {
        this.bindParameter(prepared.stmt, i + 1, query.args[i], query.argTypes[i])
      }

      const columnNames: string[] = []
      const columnTypes: ColumnType[] = []
      const rows: unknown[][] = []

      // Get column info on first step
      let stepResult = await this.sqlite3.step(prepared.stmt)

      if (stepResult === 100) { // SQLITE_ROW
        const columnCount = this.sqlite3.column_count(prepared.stmt)
        
        for (let i = 0; i < columnCount; i++) {
          columnNames.push(this.sqlite3.column_name(prepared.stmt, i))
          // We'll determine the actual type from the first row
          columnTypes.push(ColumnTypeEnum.Text) // Placeholder
        }

        // Process first row using wa-sqlite's row method
        const firstRow = this.sqlite3.row(prepared.stmt)
        rows.push(firstRow)

        // Update column types based on actual values
        for (let i = 0; i < columnCount; i++) {
          columnTypes[i] = this.inferColumnType(firstRow[i])
        }

        // Process remaining rows
        while ((stepResult = await this.sqlite3.step(prepared.stmt)) === 100) {
          rows.push(this.sqlite3.row(prepared.stmt))
        }
      }

      if (stepResult !== 101 && stepResult !== 100) { // Not SQLITE_DONE or SQLITE_ROW
        throw new Error(`SQLite step failed with code: ${stepResult}`)
      }

      return {
        columnNames,
        columnTypes,
        rows: rows.map(row => this.mapRow(row, columnTypes))
      }
    } finally {
      if (prepared) {
        await this.sqlite3.finalize(prepared.stmt)
      }
      this.sqlite3.str_finish(str)
    }
  }

  async executeRaw(query: SqlQuery): Promise<number> {
    const tag = '[wa-sqlite::executeRaw]'
    debug(`${tag} %O`, query)

    // Create a string object for the SQL
    const str = this.sqlite3.str_new(this.db, query.sql)
    let prepared: { stmt: number, sql: number } | null = null
    
    try {
      // Prepare the statement
      prepared = await this.sqlite3.prepare_v2(this.db, this.sqlite3.str_value(str))
      if (!prepared) {
        throw new Error('Failed to prepare statement')
      }

      // Bind parameters
      for (let i = 0; i < query.args.length; i++) {
        this.bindParameter(prepared.stmt, i + 1, query.args[i], query.argTypes[i])
      }

      const stepResult = await this.sqlite3.step(prepared.stmt)
      if (stepResult !== 101) { // Not SQLITE_DONE
        throw new Error(`SQLite step failed with code: ${stepResult}`)
      }

      return this.sqlite3.changes(this.db)
    } finally {
      if (prepared) {
        await this.sqlite3.finalize(prepared.stmt)
      }
      this.sqlite3.str_finish(str)
    }
  }

  private bindParameter(stmt: number, index: number, value: unknown, argType: ArgType): void {
    if (value === null || value === undefined) {
      this.sqlite3.bind(stmt, index, null)
      return
    }

    switch (argType) {
      case 'Int32':
        this.sqlite3.bind(stmt, index, Number(value))
        break
      case 'Int64':
        // wa-sqlite expects bigint for int64
        this.sqlite3.bind(stmt, index, BigInt(value as string | number))
        break
      case 'Float':
      case 'Double':
        this.sqlite3.bind(stmt, index, Number(value))
        break
      case 'Boolean':
        this.sqlite3.bind(stmt, index, value ? 1 : 0)
        break
      case 'Bytes':
        if (value instanceof Uint8Array) {
          this.sqlite3.bind(stmt, index, value)
        } else if (Array.isArray(value)) {
          this.sqlite3.bind(stmt, index, new Uint8Array(value))
        } else {
          throw new Error(`Invalid bytes value: ${value}`)
        }
        break
      default:
        this.sqlite3.bind(stmt, index, String(value))
        break
    }
  }


  private inferColumnType(value: unknown): ColumnType {
    if (value === null || value === undefined) {
      return ColumnTypeEnum.Text
    }

    switch (typeof value) {
      case 'string':
        return ColumnTypeEnum.Text
      case 'number':
        return Number.isInteger(value) ? ColumnTypeEnum.Int64 : ColumnTypeEnum.Double
      case 'boolean':
        return ColumnTypeEnum.Boolean
      case 'object':
        if (value instanceof Uint8Array) {
          return ColumnTypeEnum.Bytes
        }
        return ColumnTypeEnum.Text
      default:
        return ColumnTypeEnum.Text
    }
  }

  private mapRow(row: unknown[], columnTypes: ColumnType[]): unknown[] {
    return row.map((value, i) => {
      if (value === null || value === undefined) {
        return value
      }

      const columnType = columnTypes[i]

      // Convert Uint8Array to array of numbers for Prisma
      if (value instanceof Uint8Array) {
        return Array.from(value)
      }

      // Handle numeric types
      if (typeof value === 'number') {
        if (columnType === ColumnTypeEnum.Int32 || columnType === ColumnTypeEnum.Int64) {
          return Math.trunc(value)
        }
      }

      // Convert bigint to string
      if (typeof value === 'bigint') {
        return value.toString()
      }

      return value
    })
  }
}

class WaSQLiteTransaction extends WaSQLiteQueryable implements Transaction {
  readonly options: TransactionOptions
  private isFinished = false

  constructor(sqlite3: SQLiteAPI, db: number, options: TransactionOptions) {
    super(sqlite3, db)
    this.options = options
  }

  async commit(): Promise<void> {
    if (this.isFinished) {
      debug('[wa-sqlite::commit] Transaction already finished')
      return
    }
    
    debug('[wa-sqlite::commit]')
    try {
      const result = await this.sqlite3.exec(this.db, 'COMMIT')
      if (result !== 0) {
        throw new Error(`Failed to commit transaction: ${result}`)
      }
    } finally {
      this.isFinished = true
    }
  }

  async rollback(): Promise<void> {
    if (this.isFinished) {
      debug('[wa-sqlite::rollback] Transaction already finished')
      return
    }
    
    debug('[wa-sqlite::rollback]')
    try {
      // Check if we're in a transaction using autocommit
      // If autocommit is on (1), there's no active transaction
      const autocommit = this.sqlite3.get_autocommit ? this.sqlite3.get_autocommit(this.db) : 0
      if (autocommit) {
        debug('[wa-sqlite::rollback] No active transaction to rollback')
        return
      }
      
      const result = await this.sqlite3.exec(this.db, 'ROLLBACK')
      if (result !== 0) {
        throw new Error(`Failed to rollback transaction: ${result}`)
      }
    } catch (error: any) {
      // If rollback fails because no transaction is active, that's ok
      if (error.message && error.message.includes('no transaction is active')) {
        debug('[wa-sqlite::rollback] No active transaction, rollback skipped')
        return
      }
      throw error
    } finally {
      this.isFinished = true
    }
  }
}

class WaSQLiteAdapter extends WaSQLiteQueryable implements SqlDriverAdapter {
  constructor(sqlite3: SQLiteAPI, db: number) {
    super(sqlite3, db)
  }

  async executeScript(script: string): Promise<void> {
    debug('[wa-sqlite::executeScript] %s', script)
    const result = await this.sqlite3.exec(this.db, script)
    if (result !== 0) {
      throw new Error(`Failed to execute script: ${result}`)
    }
  }

  async startTransaction(isolationLevel?: IsolationLevel): Promise<Transaction> {
    if (isolationLevel && isolationLevel !== 'SERIALIZABLE') {
      throw new DriverAdapterError({
        kind: 'InvalidIsolationLevel',
        level: isolationLevel
      })
    }

    const options: TransactionOptions = {
      usePhantomQuery: false
    }

    debug('[wa-sqlite::startTransaction] options: %O', options)
    
    const result = await this.sqlite3.exec(this.db, 'BEGIN')
    if (result !== 0) {
      throw new Error(`Failed to start transaction: ${result}`)
    }

    return new WaSQLiteTransaction(this.sqlite3, this.db, options)
  }

  getConnectionInfo(): ConnectionInfo {
    return {
      schemaName: 'main',
      maxBindValues: 999, // SQLite default
      supportsRelationJoins: true
    }
  }

  async dispose(): Promise<void> {
    debug('[wa-sqlite::dispose]')
    await this.sqlite3.close(this.db)
  }
}

export class WaSQLiteAdapterFactory implements SqlMigrationAwareDriverAdapterFactory {
  private config: WaSQLiteConfig

  constructor(config: WaSQLiteConfig) {
    this.config = config
  }

  provider = 'sqlite' as const
  adapterName = '@banou/prisma-wa-sqlite-adapter'

  async connect(): Promise<SqlDriverAdapter> {
    if (!this.config.sqlite3) {
      throw new Error('SQLite3 instance not provided in config')
    }

    const db = await this.config.sqlite3.open_v2(this.config.url || ':memory:', 6) // SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
    if (!db) {
      throw new Error('Failed to open database')
    }

    return new WaSQLiteAdapter(this.config.sqlite3, db)
  }

  async connectToShadowDb(): Promise<SqlDriverAdapter> {
    if (!this.config.sqlite3) {
      throw new Error('SQLite3 instance not provided in config')
    }

    const db = await this.config.sqlite3.open_v2(':memory:', 6) // Always use in-memory for shadow DB
    if (!db) {
      throw new Error('Failed to open shadow database')
    }

    return new WaSQLiteAdapter(this.config.sqlite3, db)
  }
}

export async function createWaSQLitePrismaAdapter(config: Omit<WaSQLiteConfig, 'sqlite3'>): Promise<WaSQLiteAdapterFactory> {
  // Import and initialize wa-sqlite
  const { default: SQLiteWasm } = await import('wa-sqlite/dist/wa-sqlite.wasm?url')
  const SQLiteESMFactory = (await import('wa-sqlite/dist/wa-sqlite-async.mjs')).default
  const SQLite = await import('wa-sqlite')

  const module = await SQLiteESMFactory({ locateFile: () => SQLiteWasm })
  const sqlite3 = SQLite.Factory(module)

  return new WaSQLiteAdapterFactory({
    ...config,
    sqlite3
  })
}