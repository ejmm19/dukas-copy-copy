const { exec } = require('child_process');
const fs = require('fs');
const csv = require('csv-parser');
const { createObjectCsvWriter } = require('csv-writer');

// Función para obtener la hora actual
function getCurrentTime() {
    return new Date().toISOString();
}

// Redefinir console.log y console.error para incluir la hora
const originalLog = console.log;
console.log = (...args) => originalLog(`[${getCurrentTime()}]`, ...args);

const originalError = console.error;
console.error = (...args) => originalError(`[${getCurrentTime()}]`, ...args);

// Lista de instrumentos
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

// Año dinámico
const year = 2014;

const type = 'tick';
const format = 'csv';
const downloadDir = 'download'; // Directorio de descarga
const s3BucketPath = 's3://market-replay/dukascopy/forexv2/'; // Ruta S3

// Generar meses dinámicamente para el año
const months = Array.from({ length: 12 }, (_, index) => {
    const month = (index + 1).toString().padStart(2, '0');
    const nextMonth = (index + 2).toString().padStart(2, '0');
    const fromDate = `${year}-${month}-01`;
    const toDate = index === 11 ? `${year}-12-31` : `${year}-${nextMonth}-01`;

    return {
        fromDate: fromDate,
        toDate: new Date(toDate).toISOString().split('T')[0],
        name: `M${month}`
    };
});

// Ejecutar un comando en el shell de forma asíncrona
function execCommand(command) {
    return new Promise((resolve, reject) => {
        const process = exec(command);

        process.stdout.on('data', (data) => console.log(data));
        process.stderr.on('data', (error) => console.error(error));

        process.on('close', (code) => {
            if (code === 0) resolve();
            else reject(new Error(`Error en comando: ${command}. Código: ${code}`));
        });
    });
}

// Procesar un archivo CSV para agregar la columna "symbol"
async function addSymbolColumn(filePath, instrumentName) {
    const instrumentDir = `${downloadDir}/${instrumentName}`;
    const outputFilePath = `${instrumentDir}/${filePath.split('/').pop()}`;

    if (!fs.existsSync(instrumentDir)) {
        fs.mkdirSync(instrumentDir, { recursive: true });
    }

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
    return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (row) => {
                rows.push({ ...row, symbol: instrumentName.toUpperCase() });
            })
            .on('end', async () => {
                try {
                    if (rows.length === 0) {
                        throw new Error(`Archivo vacío: ${filePath}`);
                    }
                    await csvWriter.writeRecords(rows);
                    console.log(`Archivo procesado: ${outputFilePath}`);
                    resolve(outputFilePath);
                } catch (error) {
                    reject(error);
                }
            })
            .on('error', reject);
    });
}

// Subir un archivo a S3
async function uploadToS3(filePath, instrumentName) {
    const s3Path = `${s3BucketPath}${instrumentName}/`;
    const command = `aws s3 cp ${filePath} ${s3Path}`;
    console.log(`Subiendo ${filePath} a S3: ${s3Path}`);
    await execCommand(command);
}

// Descargar y procesar datos para un instrumento y un mes
async function processMonth(instrument, month) {
    const { fromDate, toDate } = month;
    const fileName = `${instrument}-${type}-${fromDate.replace(/-/g, '-')}-${toDate.replace(/-/g, '-')}.${format}`;
    const filePath = `${downloadDir}/${fileName}`;
    const command = `npx dukascopy-node -i ${instrument} -from ${fromDate} -to ${toDate} -t ${type} -f ${format} --volumes --flats --cache`;

    console.log(`Descargando datos para ${instrument} (${month.name})...`);
    await execCommand(command);

    if (!fs.existsSync(filePath) || fs.statSync(filePath).size === 0) {
        throw new Error(`Archivo no encontrado o vacío: ${filePath}`);
    }

    const processedFile = await addSymbolColumn(filePath, instrument);
    await uploadToS3(processedFile, instrument);
}

// Procesar todos los instrumentos y meses secuencialmente
async function processInstruments() {
    for (const instrument of instruments) {
        for (const month of months) {
            try {
                console.log(`Procesando ${instrument} - ${month.name}`);
                await processMonth(instrument, month);
            } catch (error) {
                console.error(`Error procesando ${instrument} (${month.name}):`, error.message);
            }
        }
    }
    console.log('Procesamiento completo.');
}

// Iniciar el procesamiento
processInstruments().catch((error) => {
    console.error('Error global:', error.message);
});
