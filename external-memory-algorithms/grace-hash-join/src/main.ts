import { createWriteStream, type WriteStream } from 'node:fs'
import { getIDPartitionIndex, getLines, getParitionsDirectory, clearPartitionsDirectory, getUpdatedFileNameForPartitionIndex } from './utils.ts'
import path from 'node:path'
import { once } from 'node:events'

const MAX_OPEN_PARITIONS = 50
const MEMORY_BUDGET = 6000000 // 6mb
const ESTIMATED_STRINGIFIED_ID_SIZE_IN_JS_SET_BYTES = 100

type FileBasicDetails = { fileName: string, filePath: string, sizeInJsBytes: number }

class StreamLRU {
	private readonly map = new Map<string, WriteStream>()
	private readonly maxOpen: number = 0

	constructor(maxOpen: number) {
		if(!maxOpen) throw new Error('maxOpen is required')

		this.maxOpen = maxOpen
	}

	async get(paritionName: string) {
		let stream = this.map.get(paritionName)
		if (stream) {
			this.map.delete(paritionName)
			this.map.set(paritionName, stream)
			return stream
		}

		const tmpDir = await getParitionsDirectory()
		const filePath = path.join(tmpDir, paritionName)
		
		if(this.map.size >= this.maxOpen) await this.evictOldest()

		stream = createWriteStream(filePath, {
			flags: 'a'
		})

		this.map.set(paritionName, stream)
		return stream
	}

	private async evictOldest() {
		const oldestStreamKey = this.map.keys().next().value
		const stream = this.map.get(oldestStreamKey)!
		stream.end()
		await once(stream, 'close')
		this.map.delete(oldestStreamKey)
	}

	async evictAll() {
		await Promise.all(
			Array.from(this.map.entries()).map(async ([key, stream]) => {
				stream.end()
				await once(stream, 'close')
				this.map.delete(key)
			})
		)
	}

	getSize() {
		return this.map.size
	}
}

class DuplicationFinder {
	private filePathA: string = ''
	private filePathB: string = ''
	private memoryBudgetBytes: number = 0
	private streamLRU: StreamLRU
	private duplicationCount = 0

	async cleanup() {
		this.filePathA = ''
		this.filePathB = ''
		this.memoryBudgetBytes = 0
		await this.streamLRU.evictAll()
		this.duplicationCount = 0
		await clearPartitionsDirectory()
	}

	constructor(filePathA: string, filePathB: string, memoryBudgetBytes: number, streamLRU: StreamLRU) {
		if(!filePathA || !filePathB) throw new Error('file paths are required!!!')
		if(!memoryBudgetBytes) throw new Error('you forgot your memory budget my friend :=)')
		if(!streamLRU) throw new Error('streamLRU is required')

		this.filePathA = filePathA
		this.filePathB = filePathB
		this.memoryBudgetBytes = memoryBudgetBytes
		this.streamLRU = streamLRU
	}

	private async countFileIds(filePath: string) {
		let count = 0
		
		for await (const id of getLines(filePath)) {
			count++
		}

		return count
	}

	async appendToParitionFile(fileName: string, idString: string) {
		const stream = await this.streamLRU.get(fileName);
		const canWrite = stream.write(idString + '\n')
		if(!canWrite) await once(stream, 'drain')
	}

	private async applyPartitioning(fileADetails: FileBasicDetails, fileBDetails: FileBasicDetails, level: number, partitionsCount: number) {
		const partitionsAIdsCount = new Uint32Array(partitionsCount)
		const partitionsBIdsCount = new Uint32Array(partitionsCount)
		
		for await (const idString of getLines(fileADetails.filePath)) {
			const partitionIndex = getIDPartitionIndex(idString, level, partitionsCount)
			
			await this.appendToParitionFile(
				getUpdatedFileNameForPartitionIndex(fileADetails.fileName, Number(partitionIndex)),
				idString
			)
			partitionsAIdsCount[Number(partitionIndex)]++
		}

		for await (const idString of getLines(fileBDetails.filePath)) {
			const partitionIndex = getIDPartitionIndex(idString, level, partitionsCount)

			await this.appendToParitionFile(
				getUpdatedFileNameForPartitionIndex(fileBDetails.fileName, Number(partitionIndex)),
				idString
			)
			partitionsBIdsCount[Number(partitionIndex)]++
		}

		await this.streamLRU.evictAll()

		for(let i = 0 ; i < partitionsCount ; i++) {
			if(partitionsBIdsCount[i] === 0 || partitionsAIdsCount[i] === 0) return

			const fileAName = getUpdatedFileNameForPartitionIndex(fileADetails.fileName, i)
			const partitionA: FileBasicDetails = {
				fileName: fileAName,
				filePath: path.join(await getParitionsDirectory(), fileAName),
				sizeInJsBytes: partitionsAIdsCount[i] * ESTIMATED_STRINGIFIED_ID_SIZE_IN_JS_SET_BYTES
			}

			const fileBName = getUpdatedFileNameForPartitionIndex(fileBDetails.fileName, i)
			const partitionB: FileBasicDetails = {
				fileName: fileBName,
				filePath: path.join(await getParitionsDirectory(), fileBName),
				sizeInJsBytes: partitionsBIdsCount[i] * ESTIMATED_STRINGIFIED_ID_SIZE_IN_JS_SET_BYTES
			}

			const smallFileToFitInSet = partitionA.sizeInJsBytes <= partitionB.sizeInJsBytes && partitionA.sizeInJsBytes <= this.memoryBudgetBytes ? partitionA : (
				partitionB.sizeInJsBytes <= partitionA.sizeInJsBytes && partitionB.sizeInJsBytes <= this.memoryBudgetBytes ? partitionB : null
			)

			if(smallFileToFitInSet === null) {
				const minPartitionSizeBytes = Math.min(partitionA.sizeInJsBytes, partitionB.sizeInJsBytes)
				const nextLevelPartitionsCount = Math.ceil(minPartitionSizeBytes / this.memoryBudgetBytes)
				await this.applyPartitioning(partitionA, partitionB, level+1, nextLevelPartitionsCount)
				continue
			}

			const largeFileForTheSet = smallFileToFitInSet.fileName === partitionA.fileName ? partitionB : partitionA

			await this.streamAndCount(smallFileToFitInSet, largeFileForTheSet)
		}
	}

	private async streamAndCount(smallFile: FileBasicDetails, largeFile: FileBasicDetails) {
		const set = new Set<string>()

		for await (const idString of getLines(smallFile.filePath)) {
			set.add(idString)
		}

		for await (const idString of getLines(largeFile.filePath)) {
			this.duplicationCount += set.has(idString) ? 1 : 0
		}
	}

	async countDuplicates() {
		const startTime = performance.now()
		try {
			const fileAIdsCount = await this.countFileIds(this.filePathA)
			const fileBIdsCount = await this.countFileIds(this.filePathB)

			const fileADetails: FileBasicDetails = {
				fileName: this.filePathA.split(/\\|\//).pop()!,
				filePath: this.filePathA,
				sizeInJsBytes: fileAIdsCount * ESTIMATED_STRINGIFIED_ID_SIZE_IN_JS_SET_BYTES
			}
			const fileBDetails: FileBasicDetails = {
				fileName: this.filePathB.split(/\\|\//).pop()!,
				filePath: this.filePathB,
				sizeInJsBytes: fileBIdsCount * ESTIMATED_STRINGIFIED_ID_SIZE_IN_JS_SET_BYTES
			}

			if(fileADetails.sizeInJsBytes <= this.memoryBudgetBytes) return await this.streamAndCount(fileADetails, fileBDetails)
			if(fileBDetails.sizeInJsBytes <= this.memoryBudgetBytes) return await this.streamAndCount(fileBDetails, fileADetails)
				
			const level = 0
			const minFileSizeBytes = Math.min(fileAIdsCount, fileBIdsCount) * ESTIMATED_STRINGIFIED_ID_SIZE_IN_JS_SET_BYTES
			const partitionsCount = Math.ceil(minFileSizeBytes / this.memoryBudgetBytes)
			await this.applyPartitioning(fileADetails, fileBDetails, level, partitionsCount)

			return this.duplicationCount

		} finally {
			await this.cleanup()
			const endTime = performance.now()
			console.log({
				memoryBudgetBytes: this.memoryBudgetBytes,
				streamLRUSize: this.streamLRU.getSize(),
				executionDurationMs: `${(endTime - startTime).toFixed(4)}ms`
			})
		}
	}
}


const streamLRUQueue = new StreamLRU(MAX_OPEN_PARITIONS)
const obj = new DuplicationFinder(
	path.join(path.dirname('./'), './test/fixtures/file_a.txt'),
	path.join(path.dirname('./'), './test/fixtures/file_b.txt'),
	MEMORY_BUDGET,
	streamLRUQueue
)

async function handleProcessTermination(error) {
	await obj.cleanup()
	console.error(error)
	process.exit(1)
}

process.on('SIGINT', handleProcessTermination)
process.on('SIGTERM', handleProcessTermination)
process.on('uncaughtException', handleProcessTermination)
process.on('uncaughtException', handleProcessTermination)

const duplicatesCount = await obj.countDuplicates()
console.log({
	duplicatesCount
})
