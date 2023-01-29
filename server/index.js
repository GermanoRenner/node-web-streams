import { createServer } from 'node:http';
import { createReadStream, statSync } from 'node:fs';
import { Readable, Transform } from 'node:stream';
import { WritableStream } from 'node:stream/web';
import { setTimeout } from 'node:timers/promises';
import { readFile } from 'node:fs';

import csvtojson from 'csvtojson'

const PORT = 3000
const FILE = 'animeflv.csv';
const FILE_PARCIAL = 'animeflv-parcial.csv';


async function streamLessResponse(response) {
    readFile(FILE, { encoding: 'utf-8' }, async function (err, data) {
        if (!err) {
            await setTimeout(1000)
            response.writeHead(200, { 'Content-Type': 'text/html' });
            response.write(data);
            response.end();
        } else {
            console.log(err);
        }
    })
}

function streamCsvToJsonNode(response) {
    createReadStream(FILE)
        .pipe(response)
}

function streamCsvToJsonWeb(request, response) {
    let items = 0;
    var fileLength = statSync(FILE)['size'];
    Readable.toWeb(createReadStream(FILE))
        //É executado em cada linha(Streams: cada linha é executada e removida da memória)
        .pipeThrough(Transform.toWeb(csvtojson()))
        .pipeThrough(new TransformStream({
            transform(chunk, controller) {
                // console.log('CHUNK >>>>>>>: ', Buffer.from(chunk).toString())
                const data = JSON.parse(Buffer.from(chunk));
                const mappedData = {
                    title: data.title,
                    description: data.description,
                    url_anime: data.url_anime
                }
                controller.enqueue(JSON.stringify(mappedData).concat('\n'))
            }
        }))
        //É o ultimo passo executado na stream
        .pipeTo(new WritableStream({
            async write(chunk) {
                await setTimeout(1000)
                items++;
                response.write(chunk);
            },
            close() {
                response.end();
            }
        }));
        request.once('close', _ => console.log('Finished with: ', items));
}

createServer(async (request, response) => {
    const headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': '*'
    }

    if (request.method == 'OPTIONS') {
        response.writeHead(204, headers);
        response.end();
        return;
    }
    console.time('TEMPO')
    streamCsvToJsonWeb(request, response);
    console.timeEnd('TEMPO');
    response.writeHead(200, headers);
    // response.end('ok');
})
    .listen(PORT)
    .on('listening', _ => console.log(`server running at ${PORT}`));
