const { exec } = require('child_process');
const fs = require('fs');
const csv = require('csv-parser');
const { createObjectCsvWriter } = require('csv-writer');

// Lista de instrumentos (Forex Currencies, Major Currencies y Metals)
const instruments = [
    // Lista completa
    'audcad', 'audchf', 'audjpy', 'audnzd', 'audsgd',
    'cadchf', 'cadhkd', 'cadjpy',
    'chfjpy', 'chfsgd',
    'euraud', 'eurcad', 'eurchf', 'eurczk', 'eurdkk', 'eurgbp', 'eurhkd', 'eurhuf',
    'eurjpy', 'eurnok', 'eurnzd', 'eurpln', 'eursek', 'eursgd', 'eurtry',
    'gbpaud', 'gbpcad', 'gbpchf', 'gbpjpy', 'gbpnzd',
    'hkdjpy',
    'nzdcad', 'nzdchf', 'nzdjpy',
    'sgdjpy',
    'tryjpy',
    'usdaed', 'usdcnh', 'usdczk', 'usddkk', 'usdhkd', 'usdhuf', 'usdils',
    'usdmxn', 'usdnok', 'usdpln', 'usdron', 'usdsar', 'usdsek', 'usdsgd',
    'usdthb', 'usdtry', 'usdzar',
    'zarjpy',
    'audusd', 'eurusd', 'gbpusd', 'nzdusd', 'usdcad', 'usdchf', 'usdjpy',
    'xagusd', 'xauusd'
];


const fromDate = '2014-01-01';
const toDate = '2014-12-31';
const type = 'tick';
const format = 'csv';
const downloadDir = 'download'; // Directorio por defecto
const s3BucketPath = 's3://market-replay/dukascopy/forexv2/'; // Ruta S3

// Función para agregar la columna "symbol"
function addSymbolColumn(filePath, instrumentName) {
    const uppercaseInstrument = instrumentName.toUpperCase();
    const outputFilePath = filePath.replace('.csv', '_with_symbol.csv');

    const csvWriter = createObjectCsvWriter({
        path: outputFilePath,
        header: [
            { id: 'timestamp', title: 'timestamp' },
            { id: 'askPrice', title: 'askPrice' },
            { id: 'bidPrice', title: 'bidPrice' },
            { id: 'askVolume', title: 'askVolume' },
            { id: 'bidVolume', title: 'bidVolume' },
            { id: 'symbol', title: 'symbol' }
        ]
    });

    const rows = [];
    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
            // Agrega la columna "symbol" a cada fila
            rows.push({ ...row, symbol: uppercaseInstrument });
        })
        .on('end', async () => {
            if (rows.length === 0) {
                console.error(`El archivo no contiene filas procesables: ${filePath}`);
                return;
            }
            try {
                await csvWriter.writeRecords(rows);
                console.log(`Archivo actualizado con la columna 'symbol': ${outputFilePath}`);
                uploadToS3(outputFilePath); // Subir el archivo a S3
            } catch (err) {
                console.error(`Error al escribir el archivo CSV actualizado: ${err}`);
            }
        })
        .on('error', (err) => {
            console.error(`Error al procesar el archivo CSV: ${err}`);
        });
}

// Función para subir un archivo a S3
function uploadToS3(filePath) {
    const command = `aws s3 cp ${filePath} ${s3BucketPath}`;
    console.log(`Subiendo ${filePath} a S3: ${s3BucketPath}`);
    const process = exec(command);

    process.stdout.on('data', (data) => console.log(data));
    process.stderr.on('data', (error) => console.error(`Error al subir a S3: ${error}`));

    process.on('close', (code) => {
        if (code === 0) {
            console.log(`Archivo subido exitosamente a S3: ${s3BucketPath}`);
        } else {
            console.error(`Error al subir archivo a S3. Código de salida: ${code}`);
        }
    });
}

// Función para descargar y procesar un instrumento
function downloadData(instrument, callback) {
    const fileName = `${instrument}-${type}-${fromDate.replace(/-/g, '-')}-${toDate.replace(/-/g, '-')}.${format}`;
    const filePath = `${downloadDir}/${fileName}`;
    const command = `npx dukascopy-node -i ${instrument} -from ${fromDate} -to ${toDate} -t ${type} -f ${format} --volumes --flats --cache`;

    console.log(`Ejecutando: ${command}`);
    const process = exec(command);

    process.stdout.on('data', (data) => console.log(data));
    process.stderr.on('data', (error) => console.error(error));

    process.on('close', (code) => {
        if (code === 0) {
            console.log(`Descarga para ${instrument} completada.`);
            if (fs.existsSync(filePath) && fs.statSync(filePath).size > 0) {
                // Agregar la columna "symbol" al archivo descargado
                addSymbolColumn(filePath, instrument);
                callback();
            } else {
                console.error(`Archivo descargado no encontrado o está vacío: ${filePath}`);
                callback();
            }
        } else {
            console.error(`Error al descargar datos para ${instrument}. Código de salida: ${code}`);
            callback();
        }
    });
}

// Función principal para iterar sobre los instrumentos
function processInstruments(index = 0) {
    if (index >= instruments.length) {
        console.log('Procesamiento de todos los instrumentos completado.');
        return;
    }

    const instrument = instruments[index];
    console.log(`Iniciando procesamiento para ${instrument}...`);
    downloadData(instrument, () => {
        processInstruments(index + 1); // Procesar el siguiente instrumento
    });
}

// Iniciar el procesamiento
processInstruments();
