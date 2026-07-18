import { createReadStream } from 'node:fs'
import { createInterface } from 'node:readline'
import { join } from 'path'

async function* getLines(filePath: string) {
	const stream = createReadStream(join(import.meta.dirname, filePath), { encoding: 'utf-8' })
	const readline = createInterface({
		input: stream,
		crlfDelay: Infinity
	})

	try {
		for await (const line of readline) {
			const txtID = line.trim()
			if(!txtID.length) continue
			yield line
		}

	} finally {
		readline.close()
		stream.destroy()
	}
}

let totalSize = 0 // bytes
for await (const line of getLines('../test/fixtures/file_a.txt')) {
	totalSize += 70
}

console.log('totalSize is: ', `${totalSize} bytes`)