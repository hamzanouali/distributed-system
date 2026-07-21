import { createReadStream } from 'node:fs'
import { mkdtemp, rm } from 'node:fs/promises'
import { createInterface } from 'node:readline'
import path, { join } from 'node:path'
import { tmpdir } from 'node:os'
import { xxh64 } from '@node-rs/xxhash'
import { parseArgs, type ParseArgsConfig } from 'node:util'

export function readCli() {
	const config: ParseArgsConfig = {
		options: {
			'file-a-path': { type: 'string' },
			'file-b-path': { type: 'string' },
			'memory-budget-bytes': { type: 'string' },
		}
	}

	return parseArgs(config).values
}

console.log(readCli())

export async function* getLines(filePath: string) {
	const stream = createReadStream(filePath, { encoding: 'utf-8' })
	const readline = createInterface({
		input: stream,
		crlfDelay: Infinity
	})

	try {
		for await (const line of readline) {
			const txtID = line.trim()
			if(!txtID.length) continue
			yield txtID
		}

	} finally {
		readline.close()
		stream.destroy()
	}
}

let partitionsDir = ''
export async function getParitionsDirectory(): Promise<string> {
	if(partitionsDir.length) return partitionsDir

	partitionsDir = await mkdtemp(join(tmpdir(), 'sarj-'))

	return partitionsDir
}

export async function clearPartitionsDirectory(): Promise<void> {
	if(!partitionsDir) return

	await rm(partitionsDir, {
		force: true,
		recursive: true,
	})
	partitionsDir = ''
}

export function modulo(a: bigint, b: bigint) {
	return (((a % b) + b) % b)
}

export function getIDPartitionIndex(id: string, level: number = 0, partitionsCount: number) {
	const hash = xxh64(`${id}:${level}`)
	return modulo(hash, BigInt(partitionsCount))
}

export function getUpdatedFileNameForPartitionIndex(fileName: string, partitionIndex: number) {
	if(!/(.*)(\.txt)$/.test(fileName)) throw new Error('file must be .txt')
	
	const parsed = path.parse(fileName)
	return `${parsed.name}.${partitionIndex}${parsed.ext || '.txt'}`
}
