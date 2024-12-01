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


// Año dinámico (puedes cambiar este valor para procesar otro año)
const year = 2014;

const type = 'tick';
const format = 'csv';
const downloadDir = 'download'; // Directorio de descarga
const s3BucketPath = 's3://market-replay/dukascopy/forexv2/'; // Ruta S3

// Generar trimestres dinámicamente para el año
const quarters = [
    { fromDate: `${year}-01-01`, toDate: `${year}-03-31`, name: 'Q1' },
    { fromDate: `${year}-04-01`, toDate: `${year}-06-30`, name: 'Q2' },
    { fromDate: `${year}-07-01`, toDate: `${year}-09-30`, name: 'Q3' },
    { fromDate: `${year}-10-01`, toDate: `${year}-12-31`, name: 'Q4' }
];

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

// Descargar y procesar datos para un instrumento y un trimestre
async function processQuarter(instrument, quarter) {
    const { fromDate, toDate } = quarter;
    const fileName = `${instrument}-${type}-${fromDate.replace(/-/g, '-')}-${toDate.replace(/-/g, '-')}.${format}`;
    const filePath = `${downloadDir}/${fileName}`;
    const command = `npx dukascopy-node -i ${instrument} -from ${fromDate} -to ${toDate} -t ${type} -f ${format} --volumes --flats --cache`;

    console.log(`Descargando datos para ${instrument} (${quarter.name})...`);
    await execCommand(command);

    if (!fs.existsSync(filePath) || fs.statSync(filePath).size === 0) {
        throw new Error(`Archivo no encontrado o vacío: ${filePath}`);
    }

    const processedFile = await addSymbolColumn(filePath, instrument);
    await uploadToS3(processedFile, instrument);
}

// Procesar todos los instrumentos y trimestres secuencialmente
async function processInstruments() {
    for (const instrument of instruments) {
        for (const quarter of quarters) {
            try {
                console.log(`Procesando ${instrument} - ${quarter.name}`);
                await processQuarter(instrument, quarter);
            } catch (error) {
                console.error(`Error procesando ${instrument} (${quarter.name}):`, error.message);
            }
        }
    }
    console.log('Procesamiento completo.');
}

// Iniciar el procesamiento
processInstruments().catch((error) => {
    console.error('Error global:', error.message);
});