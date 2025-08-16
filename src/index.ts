import prismaClient from './prisma'

console.log('prismaClient', prismaClient)

const id = crypto.randomUUID()
const barId = crypto.randomUUID()
const bazId = crypto.randomUUID()
const createdFoo = await prismaClient.foo.create({
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
console.log('createdFoo', createdFoo)
