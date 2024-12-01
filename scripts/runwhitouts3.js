const { exec } = require('child_process');
const fs = require('fs');
const csv = require('csv-parser');
const { createObjectCsvWriter } = require('csv-writer');

// Configuración del comando
const instrument = 'eurusd';
const fromDate = '2024-04-01';
const toDate = '2024-04-30';
const type = 'tick';
const format = 'csv';
const downloadDir = 'download'; // Directorio por defecto

// Función para agregar la columna "symbol"
function addSymbolColumn(filePath, instrumentName) {
    const uppercaseInstrument = instrumentName.toUpperCase();
    const outputFilePath = filePath.replace('.csv', 'formated.csv');

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
            } catch (err) {
                console.error(`Error al escribir el archivo CSV actualizado: ${err}`);
            }
        })
        .on('error', (err) => {
            console.error(`Error al procesar el archivo CSV: ${err}`);
        });
}

// Ejecuta el comando para descargar los datos
function downloadData() {
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
            } else {
                console.error(`Archivo descargado no encontrado o está vacío: ${filePath}`);
            }
        } else {
            console.error(`Error al descargar datos para ${instrument}. Código de salida: ${code}`);
        }
    });
}

// Ejecutar la descarga y el procesamiento
downloadData();
