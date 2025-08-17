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
} from '@prisma/driver-adapter-utils'
import SQLiteESMFactory from 'wa-sqlite/dist/wa-sqlite.mjs'
import * as SQLite from 'wa-sqlite'

// @ts-expect-error
import { name as packageName } from '../package.json'
import { MAX_BIND_VALUES } from './constants'
import { getColumnTypes, mapArg, mapRow } from './conversion'
import { convertDriverError } from './errors'

const debug = Debug('prisma:driver-adapter:d1')

type D1ResultsWithColumnNames = [string[], unknown[][]]
type PerformIOResult = D1ResultsWithColumnNames
type WASqliteContext = {
  module: any
  sqlite3: SQLiteAPI
  database: number
}

/**
 * Env binding for Cloudflare D1.
 */
class WASqliteQueryable<ClientT extends WASqliteContext> implements SqlQueryable {
  readonly provider = 'sqlite'
  readonly adapterName = packageName

  constructor(protected readonly context: ClientT) {}

  /**
   * Execute a query given as SQL, interpolating the given parameters.
   */
  async queryRaw(query: SqlQuery): Promise<SqlResultSet> {
    const tag = '[js::query_raw]'
    debug(`${tag} %O`, query)

    const data = await this.performIO(query)
    const convertedData = this.convertData(data as D1ResultsWithColumnNames)
    return convertedData
  }

  private convertData(ioResult: D1ResultsWithColumnNames): SqlResultSet {
    const columnNames = ioResult[0]
    const results = ioResult[1]

    if (results.length === 0) {
      return {
        columnNames: [],
        columnTypes: [],
        rows: [],
      }
    }

    const columnTypes = Object.values(getColumnTypes(columnNames, results))
    const rows = results.map((value) => mapRow(value, columnTypes))

    return {
      columnNames,
      // * Note: without Object.values the array looks like
      // * columnTypes: [ id: 128 ],
      // * and errors with:
      // * ✘ [ERROR] A hanging Promise was canceled. This happens when the worker runtime is waiting for a Promise from JavaScript to resolve, but has detected that the Promise cannot possibly ever resolve because all code and events related to the Promise's I/O context have already finished.
      columnTypes,
      rows,
    }
  }

  /**
   * Execute a query given as SQL, interpolating the given parameters and
   * returning the number of affected rows.
   * Note: Queryable expects a u64, but napi.rs only supports u32.
   */
  async executeRaw(query: SqlQuery): Promise<number> {
    const tag = '[js::execute_raw]'
    debug(`${tag} %O`, query)

    const result = await this.performIO(query, true)
    return (result as D1Response).meta.changes ?? 0
  }

  private async performIO(query: SqlQuery, executeRaw = false): Promise<PerformIOResult> {
    try {
      const params = query.args.map((arg, i) => mapArg(arg, query.argTypes[i]))
      let currentIndex = 0
      const results: { [key: string]: SQLiteCompatibleType }[] = []
      for await (const stmt of this.context.sqlite3.statements(this.context.database, query.sql)) {
        const paramCount = this.context.sqlite3.bind_parameter_count(stmt)
        this.context.sqlite3.bind_collection(stmt, params.slice(currentIndex, paramCount))
        currentIndex = currentIndex + paramCount
        while (await this.context.sqlite3.step(stmt) === SQLite.SQLITE_ROW) {
          const columnCount = this.context.sqlite3.column_count(stmt)
          const columnNames = this.context.sqlite3.column_names(stmt)
          const columnValues =
            Array(columnCount)
              .fill(undefined)
              .map((_, i) => this.context.sqlite3.column(stmt, i))
          const record =
            Object.fromEntries(
              columnNames.map((name, i) => {
                if (columnValues[i] === undefined) throw new Error(`No value found for column ${name}`)
                return [
                  name,
                  columnValues[i]
                ]
              })
            )
          results.push(record)

          return [columnNames, ...columnValues]
        }
      }
    } catch (e) {
      onError(e as Error)
    }
  }
}

class WASqliteTransaction extends WASqliteQueryable<WASqliteContext> implements Transaction {
  constructor(context: WASqliteContext, readonly options: TransactionOptions) {
    super(context)
  }

  async commit(): Promise<void> {
    debug(`[js::commit]`)
  }

  async rollback(): Promise<void> {
    debug(`[js::rollback]`)
  }
}

export class PrismaWASqliteAdapter extends WASqliteQueryable<WASqliteContext> implements SqlDriverAdapter {
  alreadyWarned = new Set()

  constructor(context: WASqliteContext, private readonly release?: () => Promise<void>) {
    super(context)
  }

  /**
   * This will warn once per transaction
   * e.g. the following two explicit transactions
   * will only trigger _two_ warnings
   *
   * ```ts
   * await prisma.$transaction([ ...queries ])
   * await prisma.$transaction([ ...moreQueries ])
   * ```
   */
  private warnOnce = (key: string, message: string, ...args: unknown[]) => {
    if (!this.alreadyWarned.has(key)) {
      this.alreadyWarned.add(key)
      console.warn(`${message}`, ...args)
    }
  }

  async executeScript(script: string): Promise<void> {
    try {
      await this.context.sqlite3.exec(this.context.database, script)
    } catch (error) {
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

    this.warnOnce(
      'D1 Transaction',
      "Cloudflare D1 does not support transactions yet. When using Prisma's D1 adapter, implicit & explicit transactions will be ignored and run as individual queries, which breaks the guarantees of the ACID properties of transactions. For more details see https://pris.ly/d/d1-transactions",
    )

    const options: TransactionOptions = {
      usePhantomQuery: true,
    }

    const tag = '[js::startTransaction]'
    debug('%s options: %O', tag, options)

    return new WASqliteTransaction(this.context, options)
  }

  async dispose(): Promise<void> {
    await this.release?.()
  }
}

export class PrismaWASqliteAdapterFactory implements SqlDriverAdapterFactory {
  readonly provider = 'sqlite'
  readonly adapterName = packageName

  constructor() {}

  async connect(): Promise<SqlDriverAdapter> {
    // @ts-expect-error
    const { default: SQLiteWasm } = await import('wa-sqlite/dist/wa-sqlite.wasm?url')
    const module = await SQLiteESMFactory({ locateFile: () => SQLiteWasm })
    const sqlite3 = SQLite.Factory(module)
    const database = await sqlite3.open_v2(':memory:')
    return new PrismaWASqliteAdapter({ module, sqlite3, database }, async () => {})
  }
}

function onError(error: Error): never {
  console.error('Error in performIO: %O', error)
  throw new DriverAdapterError(convertDriverError(error))
}
