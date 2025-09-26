const ExcelJS = require('exceljs');
const path = require('path');
const { createClient } = require('@clickhouse/client');
const cron = require('node-cron');
require('dotenv').config();
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const chClient = createClient({
  url: process.env.CLK_HS_HOST_WITH_PORT || 'http://localhost:8123',
  username: process.env.CLK_HS_USER_NAME || 'default',
  password: process.env.CLK_HS_USER_PASS || '',
  database: process.env.CLK_HS_DB || 'default',
});


async function exportChargebacks(month, startDate, endDate) {
  let sql = `
    SELECT 
        MID,
        SID,
        COUNT() AS CB_COUNT,
        SUM(toFloat64(Adjamount)) AS total_amount
    FROM CHARGEBACK_NEW
  `;

  if (month) {
    sql += ` WHERE formatDateTime(TxnDate, '%Y-%m') = '${month}'`;
  } else if (startDate && endDate) {
    sql += ` WHERE TxnDate BETWEEN '${startDate}' AND '${endDate}'`;
  } else {
    throw new Error('Provide either month OR startDate & endDate');
  }

  sql += ` GROUP BY MID, SID ORDER BY total_amount DESC`;

  const resultSet = await chClient.query({ query: sql, format: 'JSONEachRow' });
  const data = await resultSet.json();
  console.log(data)
  return data;
}

async function exportToExcel(data, month, startDate, endDate) {
  const workbook = new ExcelJS.Workbook();
  const worksheet = workbook.addWorksheet('Chargeback');

  worksheet.columns = [
    { header: 'MID', key: 'MID', width: 20 },
    { header: 'SID', key: 'SID', width: 20 },
    { header: 'CB Count', key: 'CB_COUNT', width: 15 },
    { header: 'Total Amount', key: 'total_amount', width: 20 },
  ];

  data.forEach(row => {
    worksheet.addRow({
      MID: row.MID,
      SID: row.SID,
      CB_COUNT: row.CB_COUNT,
      total_amount: row.total_amount,
    });
  });

  const grandTotal = data.reduce((acc, row) => acc + Number(row.total_amount), 0);
  const totalCBCount = data.reduce((acc, row) => acc + Number(row.CB_COUNT), 0);

  worksheet.addRow({});
  worksheet.addRow({
    MID: 'Grand Total',
    SID: '',
    CB_COUNT: totalCBCount,
    total_amount: grandTotal,
  });

  const fileName = month
    ? `Chargeback_${month}.xlsx`
    : `Chargeback_${startDate}_to_${endDate}.xlsx`;

  const filePath = path.join(process.cwd(), fileName);
  console.log('Current working directory:', process.cwd());
  console.log('Saving file as:', filePath);
  await workbook.xlsx.writeFile(filePath);
  console.log(`Excel exported: ${filePath}`);
}

// --- CLI Arguments ---
const args = yargs(hideBin(process.argv))
  .option('month', { type: 'string' })
  .option('startDate', { type: 'string' })
  .option('endDate', { type: 'string' })
  .option('cron', { type: 'boolean', describe: 'Run daily at 10 AM' })
  .help()
  .argv;
(async () => {
  try {
    if (args.cron) {
      console.log('⏳ Monthly scheduler started: 1st day of every month at 10 AM');
      cron.schedule('0 10 1 * *', async () => {
        try {
          const today = new Date();
          const lastMonth = new Date(today.getFullYear(), today.getMonth() - 1, 1);
          const monthStr = lastMonth.toISOString().slice(0, 7);

          console.log(`Exporting chargebacks for month: ${monthStr}`);
          const data = await exportChargebacks(monthStr);
          await exportToExcel(data, monthStr);
        } catch (err) {
          console.error('Cron job error:', err);
        }
      });
    }
    else if (args.month || (args.startDate && args.endDate)) {
      const data = await exportChargebacks(args.month, args.startDate, args.endDate);
      await exportToExcel(data, args.month, args.startDate, args.endDate);
    }
    else {
      console.log('Please provide either --cron or --month or --startDate & --endDate');
    }
  } catch (err) {
    console.error('Error:', err.message);
  }
})();


