import { createWaSQLitePrismaAdapter } from './wa-sqlite-adapter'
// @ts-expect-error
import SQLSchema from '../prisma/migrations/0_init/migration.sql?raw'
import { PrismaClient } from './generated/client'

const adapter = await createWaSQLitePrismaAdapter({})
const prismaClient = new PrismaClient({ adapter })
await prismaClient.$executeRawUnsafe(SQLSchema as string)

export default prismaClient