import { getLines } from './utils.ts'

function getMemoryUsedHeap() {
	global.gc?.()
	global.gc?.()
	return process.memoryUsage().heapUsed
}

const before = getMemoryUsedHeap()
const set = new Set()
for await (const line of getLines('../test/fixtures/file_b.txt')) {
	set.add(line)
	break;
}
const after = getMemoryUsedHeap()

console.log(`Total memory used for the set: ${after-before} bytes`)
console.log(`Memory used for each set item (total items = ${set.size}): ${((after-before) / set.size)} bytes`)