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

// Lista de instrumentos (puedes agregar más si lo deseas)
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
const year = 2022;

const type = 'tick';
const format = 'csv';
const downloadDir = 'download'; // Directorio de descarga
const s3BucketPath = 's3://market-replay/dukascopy/forexv2/'; // Ruta S3

// Función para generar rangos diarios para todo el año
function generateDailyDateRanges(year) {
    const ranges = [];
    const daysInMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    // Ajustar febrero si el año es bisiesto
    if ((year % 4 === 0 && year % 100 !== 0) || year % 400 === 0) {
        daysInMonth[1] = 29; // Año bisiesto
    }

    let periodCounter = 1;
    for (let month = 0; month < 12; month++) {
        const totalDays = daysInMonth[month];
        for (let day = 1; day <= totalDays; day++) {
            const fromDate = `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
            ranges.push({
                fromDate,
                toDate: fromDate,
                name: `P${periodCounter}`
            });
            periodCounter++;
        }
    }
    return ranges;
}

// Generar los rangos diarios para el año
const dateRanges = generateDailyDateRanges(year);

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

// Verificar si un archivo existe en S3
async function fileExistsInS3(filePath) {
    const command = `aws s3 ls ${filePath}`;
    try {
        await execCommand(command);
        console.log(`El archivo ya existe en S3: ${filePath}`);
        return true; // El archivo existe en S3
    } catch {
        console.log(`El archivo no existe en S3: ${filePath}`);
        return false; // El archivo no existe en S3
    }
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

// Descargar y procesar datos para un instrumento y un rango de fechas
async function processDateRange(instrument, range) {
    const { fromDate, toDate, name } = range;
    const fileName = `${instrument}-${type}-${fromDate.replace(/-/g, '-')}-${toDate.replace(/-/g, '-')}.${format}`;
    const localFilePath = `${downloadDir}/${fileName}`;
    const s3FilePath = `${s3BucketPath}${instrument}/${fileName}`;

    // Validar si el archivo ya existe en S3
    if (await fileExistsInS3(s3FilePath)) {
        console.log(`Archivo ya existe en S3: ${s3FilePath}. Omitiendo procesamiento.`);
        return; // Salir de la función si el archivo ya existe en S3
    }

    const command = `npx dukascopy-node -i ${instrument} -from ${fromDate} -to ${toDate} -t ${type} -f ${format} --volumes --flats --cache`;

    console.log(`Descargando datos para ${instrument} (${name})...`);
    await execCommand(command);

    if (!fs.existsSync(localFilePath) || fs.statSync(localFilePath).size === 0) {
        throw new Error(`Archivo no encontrado o vacío: ${localFilePath}`);
    }

    const processedFile = await addSymbolColumn(localFilePath, instrument);
    await uploadToS3(processedFile, instrument);
}

// Procesar todos los instrumentos y rangos de fechas secuencialmente
async function processInstruments() {
    for (const instrument of instruments) {
        for (const range of dateRanges) {
            try {
                console.log(`Procesando ${instrument} - ${range.name}`);
                await processDateRange(instrument, range);
            } catch (error) {
                console.error(`Error procesando ${instrument} (${range.name}):`, error.message);
            }
        }
    }
    console.log('Procesamiento completo.');
}

// Iniciar el procesamiento
processInstruments().catch((error) => {
    console.error('Error global:', error.message);
});
