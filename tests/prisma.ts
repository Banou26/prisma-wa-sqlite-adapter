import { use, expect } from 'chai'
import chaiAsPromised from 'chai-as-promised'
import chaiShallowDeepEqual from 'chai-shallow-deep-equal'

use(chaiAsPromised)
use(chaiShallowDeepEqual)

export const importPrismaClient = async () => {
  await expect(import('../src/prisma')).to.be.fulfilled
}

export const count = async () => {
  const { default: client } = await import('../src/prisma')
  await expect(client.foo.count()).to.eventually.equal(0)
}

export const create = async () => {
  const { default: client } = await import('../src/prisma')

  const id = crypto.randomUUID()
  const createdFoo = await client.foo.create({
    data: {
      id
    }
  })

  expect(createdFoo).to.shallowDeepEqual({
    id,
  })
}


export const createWithRelations = async () => {
  const { default: client } = await import('../src/prisma')

  const id = crypto.randomUUID()
  const barId = crypto.randomUUID()
  const bazId = crypto.randomUUID()
  const createdFoo = await client.foo.create({
    data: {
      id,
      bar: {
        create: {
          id: barId
        }
      },
      baz: {
        create: {
          id: bazId
        }
      }
    },
    include: {
      bar: true,
      baz: true
    }
  })

  expect(createdFoo).to.shallowDeepEqual({
    id,
    bar: {
      id: barId
    },
    baz: {
      id: bazId
    }
  })
}
