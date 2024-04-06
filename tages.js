const fs = require('fs');
const readline = require('readline');
const { promisify } = require('util');

const TEMP_DIR = './temp';

const readFileAsync = promisify(fs.readFile);
const writeFileAsync = promisify(fs.writeFile);
const unlinkAsync = promisify(fs.unlink);

async function splitAndSort(inputFilePath, chunkSize) {
    const chunks = [];
    let chunk = [];
    const rl = readline.createInterface({
        input: fs.createReadStream(inputFilePath),
        crlfDelay: Infinity
    });

    for await (const line of rl) {
        chunk.push(line);
        if (chunk.length >= chunkSize) {
            chunk.sort();
            chunks.push(chunk);
            chunk = [];
        }
    }

    if (chunk.length > 0) {
        chunk.sort();
        chunks.push(chunk);
    }

    return chunks;
}

async function mergeSortedFiles(sortedFiles, outputFilePath) {
    const writers = sortedFiles.map(filePath => readline.createInterface({
        input: fs.createReadStream(filePath),
        crlfDelay: Infinity
    }));

    const mergedStream = readline.createInterface({
        input: require('stream').Readable.from(
            mergeStreams(writers.map(w => w[Symbol.asyncIterator]()))
        ),
        crlfDelay: Infinity
    });

    const writeStream = fs.createWriteStream(outputFilePath);
    for await (const line of mergedStream) {
        writeStream.write(line + '\n');
    }

    writeStream.close();
}

async function* mergeStreams(streams) {
    const values = [];
    for (const stream of streams) {
        for await (const line of stream) {
            values.push(line);
        }
    }
    
    values.sort();
    for (const value of values) {
        yield value;
    }
}

async function externalSort(inputFilePath, outputFilePath, chunkSize) {
    if (!fs.existsSync(TEMP_DIR)) {
        fs.mkdirSync(TEMP_DIR);
    }

    const chunks = await splitAndSort(inputFilePath, chunkSize);

    const tempFiles = [];
    for (let i = 0; i < chunks.length; i++) {
        const tempFilePath = `${TEMP_DIR}/temp_${i}.txt`;
        await writeFileAsync(tempFilePath, chunks[i].join('\n'));
        tempFiles.push(tempFilePath);
    }

    await mergeSortedFiles(tempFiles, outputFilePath);

    // Удаление временных файлов
    for (const tempFile of tempFiles) {
        await unlinkAsync(tempFile);
    }
    fs.rmdirSync(TEMP_DIR);
}

// Пример использования:
externalSort('input_file.txt', 'sorted_output.txt', 100000);
