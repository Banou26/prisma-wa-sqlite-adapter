import { PrismaClient } from './generated/client'
import Database from './wa-sqlite-better-sqlite3'
// @ts-expect-error
import SQLSchema from '../prisma/migrations/0_init/migration.sql?raw'

// Import our factory implementation
import { createWaSQLiteAdapterFactory } from './wa-sqlite-factory'

// Create the database and adapter
async function createPrismaClient() {
  // Create our better-sqlite3 compatible database using wa-sqlite
  const database = await Database(':memory:')
  
  // Execute the schema
  database.exec(SQLSchema as string)
  
  // Create the adapter factory
  const adapter = createWaSQLiteAdapterFactory(database)
  
  // Create PrismaClient with the adapter
  const prismaClient = new PrismaClient({ adapter })
  
  return prismaClient
}

const prismaClient = await createPrismaClient()

export default prismaClient