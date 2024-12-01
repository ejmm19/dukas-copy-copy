const { exec } = require('child_process');
const { spawn } = require('child_process');
const fs = require('fs');

const instruments = [
    // Forex Currencies
    'audcad', 'audchf', 'audjpy', 'audnzd', 'audusd', 'cadchf', 'cadjpy',
    'chfjpy', 'euraud', 'eurcad', 'eurchf', 'eurgbp', 'eurjpy', 'eurnzd',
    'eurusd', 'gbpaud', 'gbpcad', 'gbpchf', 'gbpjpy', 'gbpnzd', 'gbpusd',
    'nzdcad', 'nzdchf', 'nzdjpy', 'nzdusd', 'usdcad', 'usdchf', 'usdjpy',
    // Forex Major Currencies
    'eurusd', 'usdjpy', 'gbpusd', 'audusd', 'usdcad', 'usdchf',
    // Forex Metals
    'xauusd', 'xagusd'
];

const bucketName = 'dukascopy-data'; // Nombre de tu bucket
const type = 'tick';
const format = 'csv';
const startYear = 2014;
const endYear = 2020;

// Función para subir un archivo a S3
function uploadToS3(filePath, s3Path) {
    console.log(`Subiendo ${filePath} a S3...`);
    const aws = spawn('aws', ['s3', 'cp', filePath, `s3://${bucketName}/${s3Path}`]);

    aws.stdout.on('data', (data) => console.log(`AWS CLI: ${data}`));
    aws.stderr.on('data', (error) => console.error(`Error AWS CLI: ${error}`));

    aws.on('close', (code) => {
        if (code === 0) {
            console.log(`Archivo ${filePath} subido exitosamente.`);
            // Elimina el archivo local para ahorrar espacio
            fs.unlink(filePath, (err) => {
                if (err) console.error(`Error al eliminar ${filePath}: ${err}`);
                else console.log(`Archivo local ${filePath} eliminado.`);
            });
        } else {
            console.error(`Error al subir ${filePath}. Código de salida: ${code}`);
        }
    });
}

// Función para ejecutar el comando de descarga
function downloadData(instrument, yearIndex = 0) {
    const year = startYear + yearIndex;
    if (year > endYear) {
        console.log(`Descargas para ${instrument} completadas.`);
        return;
    }

    const fromDate = `${year}-01-01`;
    const toDate = `${year}-12-31`;
    const outputFileName = `${instrument}_${year}.csv`;

    const command = `npx dukascopy-node -i ${instrument} -from ${fromDate} -to ${toDate} -t ${type} -f ${format} -o ${outputFileName}`;
    console.log(`Ejecutando: ${command}`);

    const process = exec(command);

    process.stdout.on('data', (data) => console.log(data));
    process.stderr.on('data', (error) => console.error(error));

    process.on('close', (code) => {
        if (code === 0) {
            console.log(`Descarga para ${instrument} en ${year} completada.`);
            // Subir a S3 y continuar con el siguiente año
            uploadToS3(outputFileName, `${instrument}/${outputFileName}`);
            downloadData(instrument, yearIndex + 1);
        } else {
            console.error(`Error al descargar datos para ${instrument} en ${year}. Código de salida: ${code}`);
        }
    });
}

// Función principal para recorrer los instrumentos
function downloadAllInstruments(index = 0) {
    if (index >= instruments.length) {
        console.log('Todas las descargas han terminado.');
        return;
    }

    const instrument = instruments[index];
    console.log(`Iniciando descargas para ${instrument}...`);
    downloadData(instrument, 0);

    // Continuar con el siguiente instrumento cuando termine el actual
    const checkNextInstrument = setInterval(() => {
        if (!execSync(`pgrep -f "dukascopy-node -i ${instrument}"`, { encoding: 'utf8', stdio: 'ignore' })) {
            clearInterval(checkNextInstrument);
            downloadAllInstruments(index + 1);
        }
    }, 5000); // Verificar cada 5 segundos
}

// Inicia la descarga para todos los instrumentos
downloadAllInstruments();