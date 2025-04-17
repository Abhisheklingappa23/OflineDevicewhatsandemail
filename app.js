const express = require('express');
const app = express()
var sql = require("mssql");
const bodyParser = require('body-parser')
var fs = require('fs');
const path = require('path');
const axios = require("axios");

const { promisify } = require('util');
const nodemailer = require('nodemailer');
const dedent = require('dedent');
app.use(express.json())
app.use(bodyParser.json());


const sqlConfig = {
    user: 'cultyvateproduction',
    password: '#(Cult!Y1@va4t$$e9!$@8#$^',
    database: 'cultYvate_Live',
    server: 'SQL.cultyvate.com',
    connectionTimeout: 300000,
    requestTimeout: 300000,
    pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 300000
    },
    options: {
        encrypt: true, // for azure
        trustServerCertificate: true // change to true for local dev / self-signed certs
    }
}


setInterval(() => {

    let l_Date_ObJ = new Date();   
    var l_Current_Date_Time;
    var lTriggerRequiredHrs = "14:34";
    

    l_Current_Date_Time = l_Date_ObJ.getFullYear() + "-"+ (l_Date_ObJ.getMonth() + 1) + "-" + l_Date_ObJ.getDate() + " " + l_Date_ObJ.getHours() + ":" + l_Date_ObJ.getMinutes()
    var lTriggerDateTime = l_Date_ObJ.getFullYear() + "-"+ (l_Date_ObJ.getMonth() + 1) + "-" + l_Date_ObJ.getDate()+" "+ lTriggerRequiredHrs
    
    //F2FFarmerGetData()
    if(lTriggerDateTime==l_Current_Date_Time){
    //F2FWriteToLogFile(" Trigger Time Mached : " + lTriggerDateTime + "=" + l_Current_Date_Time)    
    F2FFarmerGetData()
    
    }
    

    const transporter = nodemailer.createTransport({
        service: 'gmail',
        auth: {
        user: 'cultyvate.hello@gmail.com',
        pass: 'tetpdfslslylrpfs'
        }
    });

    async function parseDeviceRecords(deviceGroupString) {
        // Split the entire string on semicolon and remove empty entries
        const records = deviceGroupString.split(";")
          .map(item => item.trim())
          .filter(item => item !== "");
      
        // Regular expression to capture each field.
        // Note: Adjust spacing if necessary. This regex expects a fixed order.
        const regex = /^(?<sno>\d+)\.\s*DeviceType:\s*(?<DeviceType>[^,]+),\s*DeviceID:\s*(?<DeviceID>[^,]+),\s*Farmer:\s*(?<Farmer>[^,]+),\s*FarmerNumber:\s*(?<FarmerNumber>[^,]+),\s*Village:\s*(?<Village>[^,]+),\s*GatewayUpdatedTime:\s*(?<GatewayUpdatedTime>[^,]+),\s*OfflineSinceFrom:\s*(?<OfflineSinceFrom>.+)$/i;
      
        const parsedRecords = [];
      
        for (let rec of records) {
          const match = rec.match(regex);
          if (match && match.groups) {
            parsedRecords.push({
              sno: match.groups.sno.trim(),
              DeviceType: match.groups.DeviceType.trim(),
              DeviceID: match.groups.DeviceID.trim(),
              Farmer: match.groups.Farmer.trim(),
              FarmerNumber: match.groups.FarmerNumber.trim(),
              Village: match.groups.Village.trim(),
              GatewayUpdatedTime: match.groups.GatewayUpdatedTime.trim(),
              OfflineSinceFrom: match.groups.OfflineSinceFrom.trim()
            });
          } else {
            // Fallback: If regex fails, push a record with the raw string.
            parsedRecords.push({ raw: rec });
          }
        }
        return parsedRecords;
    }

    async function buildHtmlEmailFromDeviceGroup_(fieldOfficerName, deviceGroupString) {
        // Parse the device records asynchronously
        const parsedRecords = await parseDeviceRecords(deviceGroupString);
      
        // Build table header
        const tableHeader = `
          <tr style="background:#f2f2f2;">
            <th style="border:1px solid #ccc; padding:5px;">S.No</th>
            <th style="border:1px solid #ccc; padding:5px;">DeviceType</th>
            <th style="border:1px solid #ccc; padding:5px;">DeviceID</th>
            <th style="border:1px solid #ccc; padding:5px;">Farmer</th>
            <th style="border:1px solid #ccc; padding:5px;">FarmerNumber</th>
            <th style="border:1px solid #ccc; padding:5px;">Village</th>
            <th style="border:1px solid #ccc; padding:5px;">DeviceLastcommunicated</th>
            <th style="border:1px solid #ccc; padding:5px;">OfflineSinceFrom</th>
          </tr>`;
      
        // Build table rows by mapping over parsedRecords
        const tableRows = parsedRecords.map(record => {
          // If record parsing failed, show the raw string in a full-row cell
          if (record.raw) {
            return `<tr><td colspan="8" style="border:1px solid #ccc; padding:5px;">${record.raw}</td></tr>`;
          }
      
          return `
            <tr>
              <td style="border:1px solid #ccc; padding:5px;">${record.sno}</td>
              <td style="border:1px solid #ccc; padding:5px;">${record.DeviceType}</td>
              <td style="border:1px solid #ccc; padding:5px;">${record.DeviceID}</td>
              <td style="border:1px solid #ccc; padding:5px;">${record.Farmer}</td>
              <td style="border:1px solid #ccc; padding:5px;">${record.FarmerNumber}</td>
              <td style="border:1px solid #ccc; padding:5px;">${record.Village}</td>
              <td style="border:1px solid #ccc; padding:5px;">${record.GatewayUpdatedTime}</td>
              <td style="border:1px solid #ccc; padding:5px;">${record.OfflineSinceFrom}</td>
            </tr>`;
        }).join('');
      
        // Wrap header and rows in a table element
        const tableHtml = `
          <table style="border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; font-size: 14px;">
            <thead>${tableHeader}</thead>
            <tbody>${tableRows}</tbody>
          </table>`;
      
        // Return the complete HTML email content
        return dedent(`
          <p>Dear ${fieldOfficerName},</p>
          <p>Your Offline Device Details are as follows:</p>
          ${tableHtml}
          <p>Devices offline immediate action required.</p>
          <p>Regards,<br/>Team CultYvate</p>
        `);
    }

    async function buildHtmlEmailFromDeviceGroup(fieldOfficerName, deviceGroupString) {
        // Parse the device records asynchronously
        const parsedRecords = await parseDeviceRecords(deviceGroupString);
      
        // Build table header with centered text
        const tableHeader = `
          <tr style="background: #f2f2f2;">
            <th style="border: 1px solid #ccc; padding: 5px; text-align: center;">S.No</th>
            <th style="border: 1px solid #ccc; padding: 5px; text-align: center;">DeviceType</th>
            <th style="border: 1px solid #ccc; padding: 5px; text-align: center;">DeviceID</th>
            <th style="border: 1px solid #ccc; padding: 5px; text-align: center;">Farmer</th>
            <th style="border: 1px solid #ccc; padding: 5px; text-align: center;">FarmerNumber</th>
            <th style="border: 1px solid #ccc; padding: 5px; text-align: center;">Village</th>
            <th style="border: 1px solid #ccc; padding: 5px; text-align: center;">DeviceLastcommunicated</th>
            <th style="border: 1px solid #ccc; padding: 5px; text-align: center;">OfflineSinceFrom</th>
          </tr>`;
      
        // Build table rows by mapping over parsedRecords
        const tableRows = parsedRecords.map(record => {
          // If record parsing failed, show the raw string in a full-row cell
          if (record.raw) {
            return `<tr><td colspan="8" style="border: 1px solid #ccc; padding: 5px; text-align: center;">${record.raw}</td></tr>`;
          }
      
          return `
            <tr>
              <td style="border: 1px solid #ccc; padding: 5px; text-align: center;">${record.sno}</td>
              <td style="border: 1px solid #ccc; padding: 5px; text-align: center;">${record.DeviceType}</td>
              <td style="border: 1px solid #ccc; padding: 5px; text-align: center;">${record.DeviceID}</td>
              <td style="border: 1px solid #ccc; padding: 5px; text-align: center;">${record.Farmer}</td>
              <td style="border: 1px solid #ccc; padding: 5px; text-align: center;">${record.FarmerNumber}</td>
              <td style="border: 1px solid #ccc; padding: 5px; text-align: center;">${record.Village}</td>
              <td style="border: 1px solid #ccc; padding: 5px; text-align: center;">${record.GatewayUpdatedTime}</td>
              <td style="border: 1px solid #ccc; padding: 5px; text-align: center;">${record.OfflineSinceFrom}</td>
            </tr>`;
        }).join('');
      
        // Wrap header and rows in a table element with overall centered text
        const tableHtml = `
          <table style="border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; font-size: 14px; text-align: center;">
            <thead>${tableHeader}</thead>
            <tbody>${tableRows}</tbody>
          </table>`;
      
        // Return the complete HTML email content
        return dedent(`
          <p>Dear ${fieldOfficerName},</p>
          <p>Your Offline Device Details are as follows:</p>
          ${tableHtml}
          <p>Devices offline immediate action required.</p>
          <p>Regards,<br/>Team CultYvate</p>
        `);
    }
      


async function F2FFarmerGetData(){

    try{
        
        await sql.connect(sqlConfig)
        var farmerrecordset = await sql.query(`
            WITH DeviceCTE AS (
                -- First Section: FL records
                SELECT 
                    FO.Name AS FieldOfficer,        
                    CASE 
                        WHEN LEN(CAST(FO.MobileNumber1 AS VARCHAR)) = 10 THEN '91' + CAST(FO.MobileNumber1 AS VARCHAR)
                        WHEN LEN(CAST(FO.MobileNumber1 AS VARCHAR)) = 12 THEN CAST(FO.MobileNumber1 AS VARCHAR)
                        ELSE '918073796903'
                    END AS FieldOfficerNumber,
                    FO.MobileNumber1 AS FormattedMobileNumber,
                    CASE
                      WHEN FO.fEmail IS NULL OR LTRIM(RTRIM(FO.fEmail)) = '' THEN 'contactmrabhishekl@gmail.com'
                      ELSE FO.fEmail
                    END AS FOEmail,
                    DT.Name AS DeviceType,
                    FDD.DeviceEuIID,
                    FDD.DeviceTypeID, 
                    Farmer.Name AS FarmerName,
                    Farmer.FatherName,
                    Farmer.MobileNumberPrimary,
                    Village.Name AS VillageName,
                    CONVERT(VARCHAR(20), TDS.CreateDate, 120) AS GatewayUpdatedTime,                                        
                    CASE
                        WHEN DATEDIFF(HOUR, TDS.CreateDate, DATEADD(MINUTE, 330, GETUTCDATE())) >= 24 THEN 
                            CONVERT(VARCHAR, DATEDIFF(DAY, TDS.CreateDate, DATEADD(MINUTE, 330, GETUTCDATE()))) 
                            + CASE 
                                WHEN DATEDIFF(DAY, TDS.CreateDate, DATEADD(MINUTE, 330, GETUTCDATE())) = 1 
                                THEN ' Day' 
                                ELSE ' Days' 
                              END
                        ELSE 
                            CONVERT(VARCHAR, DATEDIFF(HOUR, TDS.CreateDate, DATEADD(MINUTE, 330, GETUTCDATE()))) 
                            + CASE 
                                WHEN DATEDIFF(HOUR, TDS.CreateDate, DATEADD(MINUTE, 330, GETUTCDATE())) = 1 
                                THEN ' Hour' 
                                ELSE ' Hours' 
                              END
                    END AS OfflineSinceFrom
                FROM 
                    Farmer
                    INNER JOIN FiledOfficer AS FO 
                        ON Farmer.FiledOfficeID = FO.ID
                    INNER JOIN Village 
                        ON Farmer.VillageID = Village.ID
                    INNER JOIN FarmerFarmLandDetails 
                        ON Farmer.ID = FarmerFarmLandDetails.FarmerID
                    INNER JOIN FarmerDeviceDetails AS FDD 
                        ON FarmerFarmLandDetails.ID = FDD.FarmerSectionDetailsID
                    INNER JOIN DeviceType AS DT 
                        ON FDD.DeviceTypeID = DT.ID
                    INNER JOIN TelematicsDeviceLiveStatus AS TDS 
                        ON FDD.DeviceEuIID = TDS.DeviceID
                WHERE 
                    Farmer.ClientID = 55
                    AND FDD.DeviceTypeID NOT IN (55)
                    AND FDD.FarmerSectionType = 'FL'   
                    AND FDD.DeleteYN=0
                    AND FDD.ActiveDeviceYN=1
                    AND FDD.ServiceTypeID NOT IN (6)                 
                    AND DATEDIFF(MINUTE, TDS.CreateDate, DATEADD(MINUTE, 330, GETUTCDATE())) >= 300

                UNION ALL

                -- Second Section: PO records
                SELECT 
                    FO.Name AS FieldOfficer,        
                    CASE 
                        WHEN LEN(CAST(FO.MobileNumber1 AS VARCHAR)) = 10 THEN '91' + CAST(FO.MobileNumber1 AS VARCHAR)
                        WHEN LEN(CAST(FO.MobileNumber1 AS VARCHAR)) = 12 THEN CAST(FO.MobileNumber1 AS VARCHAR)
                        ELSE '918073796903'
                    END AS FieldOfficerNumber,
                    FO.MobileNumber1 AS FormattedMobileNumber,
                    CASE
                      WHEN FO.fEmail IS NULL OR LTRIM(RTRIM(FO.fEmail)) = '' THEN 'contactmrabhishekl@gmail.com'
                      ELSE FO.fEmail
                    END AS FOEmail,
                    DT.Name AS DeviceType,
                    FDD.DeviceEuIID,
                    FDD.DeviceTypeID, 
                    Farmer.Name AS FarmerName,
                    Farmer.FatherName,
                    Farmer.MobileNumberPrimary,
                    Village.Name AS VillageName,
                    CONVERT(VARCHAR(20), TDS.CreateDate, 120) AS GatewayUpdatedTime,                                        
                    CASE
                        WHEN DATEDIFF(HOUR, TDS.CreateDate, DATEADD(MINUTE, 330, GETUTCDATE())) >= 24 THEN 
                            CONVERT(VARCHAR, DATEDIFF(DAY, TDS.CreateDate, DATEADD(MINUTE, 330, GETUTCDATE()))) 
                            + CASE 
                                WHEN DATEDIFF(DAY, TDS.CreateDate, DATEADD(MINUTE, 330, GETUTCDATE())) = 1 
                                THEN ' Day' 
                                ELSE ' Days' 
                              END
                        ELSE 
                            CONVERT(VARCHAR, DATEDIFF(HOUR, TDS.CreateDate, DATEADD(MINUTE, 330, GETUTCDATE()))) 
                            + CASE 
                                WHEN DATEDIFF(HOUR, TDS.CreateDate, DATEADD(MINUTE, 330, GETUTCDATE())) = 1 
                                THEN ' Hour' 
                                ELSE ' Hours' 
                              END
                    END AS OfflineSinceFrom
                FROM 
                    Farmer
                    INNER JOIN FiledOfficer AS FO 
                        ON Farmer.FiledOfficeID = FO.ID
                    INNER JOIN Village 
                        ON Farmer.VillageID = Village.ID
                    INNER JOIN FarmerFarmLandDetails 
                        ON Farmer.ID = FarmerFarmLandDetails.FarmerID
                    INNER JOIN FarmerBlockDetails 
                        ON FarmerFarmLandDetails.ID = FarmerBlockDetails.FarmerFarmLandDetailsID
                    INNER JOIN FarmerPlotsDetails 
                        ON FarmerBlockDetails.ID = FarmerPlotsDetails.FarmerBlockDetailsID
                    INNER JOIN FarmerDeviceDetails AS FDD 
                        ON FarmerPlotsDetails.ID = FDD.FarmerSectionDetailsID
                    INNER JOIN DeviceType AS DT 
                        ON FDD.DeviceTypeID = DT.ID
                    INNER JOIN TelematicsDeviceLiveStatus AS TDS 
                        ON FDD.DeviceEuIID = TDS.DeviceID
                WHERE 
                    Farmer.ClientID = 55  
                    AND FDD.DeviceTypeID NOT IN (87)                    
                    AND DATEDIFF(MINUTE, TDS.CreateDate, DATEADD(MINUTE, 330, GETUTCDATE())) >= 300
                    AND FDD.FarmerSectionType = 'PO'
                    AND FDD.DeleteYN=0
                    AND FDD.ActiveDeviceYN=1
                    AND FDD.ServiceTypeID NOT IN (6)
            ),
            OrderedDevices AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY FieldOfficer, FormattedMobileNumber, FOEmail 
                        ORDER BY DeviceTypeID, DeviceEuIID  -- ordering by DeviceTypeID, then DeviceEuIID
                    ) AS SerialNo
                FROM DeviceCTE
            )
        SELECT 
            FieldOfficer,
            FieldOfficerNumber,
            FormattedMobileNumber,
            FOEmail,
            COUNT(DeviceEuIID) AS TotalDevicesDown,
            STRING_AGG(
                CAST(CONCAT(
                    SerialNo, '. ',
                    'DeviceType: ', DeviceType,
                    ', DeviceID: ', DeviceEuIID,
                    ', Farmer: ', FarmerName, ' S/O ', FatherName,
                    ', FarmerNumber: ', MobileNumberPrimary,
                    ', Village: ', VillageName,
                    ', GatewayUpdatedTime: ', GatewayUpdatedTime,
                    ', OfflineSinceFrom: ', OfflineSinceFrom
                ) AS VARCHAR(MAX)),
                '; '
            ) AS DeviceGroup
        FROM OrderedDevices
        GROUP BY 
            FieldOfficer, 
            FieldOfficerNumber, 
            FormattedMobileNumber,
            FOEmail
        ORDER BY FieldOfficer;

            `)
        
        var Pulled_record_of_farmer = (farmerrecordset.recordsets[0])
        var DgFarmerDetails = Pulled_record_of_farmer
        if(DgFarmerDetails.length==0){
            await F2FWriteToLogFile("No Active Farmer Data Found - Exit Program")                
        }else{
                                  
                for(let i=0;i<DgFarmerDetails.length;i++){

                    
                    
            
                    //const deviceGroupFormatted = DgFarmerDetails[i].DeviceGroup.replace(/;\s*/g, ';\n');
                    const deviceGroupFormatted = DgFarmerDetails[i].DeviceGroup.replace(/;\s*/g, '\n');

                    const messageBody = dedent(`
                    Dear ${DgFarmerDetails[i].FieldOfficer},

                    Your Device Details:
                    ${deviceGroupFormatted}

                    Devices offline immediate action required.
                    `);


                    //messageBody For whats app
                    await F2FWriteToLogFile(messageBody)

                    let DeviceGroupFormattedForEmail = DgFarmerDetails[i].DeviceGroup.replace(/;\s*/g, '\n');

                    
                    const htmlContent = await buildHtmlEmailFromDeviceGroup(DgFarmerDetails[i].FieldOfficer, DgFarmerDetails[i].DeviceGroup);
                    

                    let DbDatamailOptions = {
                        from: 'cultyvate.hello@gmail.com',
                        to: ['abhisheklingappa23@gmail.com'],                        
                        subject: 'Alert Devices Offline! Immediate Action Required',
                        html: htmlContent                        
                    };                    
            
                    try {
                        const info = await transporter.sendMail(DbDatamailOptions);
                        console.log('Email sent:', info.response);
                        await F2FWriteToLogFile('Email sent: '+ info.response)
                      } catch(err) {
                        console.error('Error sending email:', err);
                        await F2FWriteToLogFile('Error sending email: '+ err)
                      }                    
                }                
        }        
    }
    catch(err){
        await F2FWriteToLogFile(err)
    }

}



function F2FWriteToLogFile_(pLineData) {   
        
  let lDateObJ = new Date();                
  var lDayWiseLogfile = 'LOGS' + lDateObJ.getFullYear() + "-"+ (lDateObJ.getMonth()+1) + "-" + lDateObJ.getDate() +'.txt'                
  const filePath = path.join(__dirname, 'LogsCB',lDayWiseLogfile); 
  var lLogFileTime =  ("0" + lDateObJ.getDate()).slice(-2) +"-"+ ("0" + (lDateObJ.getMonth() + 1)).slice(-2) + "-" +lDateObJ.getFullYear() +  " " + ("0" +lDateObJ.getHours()).slice(-2) + ":" + ("0" +lDateObJ.getMinutes()).slice(-2) + ":" + ("0" +lDateObJ.getSeconds()).slice(-2)
  var lLogData = lLogFileTime + " " + pLineData + "\n"
  fs.appendFileSync(filePath, lLogData, 'utf8');
}

async function F2FWriteToLogFile(pLineData) {   
    try {
      let lDateObJ = new Date();
      // Build the logfile name
      let lDayWiseLogfile = 'LOGS' + lDateObJ.getFullYear() + "-" + (lDateObJ.getMonth() + 1) + "-" + lDateObJ.getDate() + '.txt';
  
      // Use path.resolve to define the log folder robustly.
      // Change '..' to adjust the folder level if required.
      const logsFolder = path.resolve(__dirname, 'LogsCB');
  
      // Ensure the folder exists
      await fs.promises.mkdir(logsFolder, { recursive: true });
  
      // Build the full log file path
      const filePath = path.join(logsFolder, lDayWiseLogfile);
  
      // Build the timestamp
      let lLogFileTime = ("0" + lDateObJ.getDate()).slice(-2) + "-" +
                          ("0" + (lDateObJ.getMonth() + 1)).slice(-2) + "-" +
                          lDateObJ.getFullYear() + " " +
                          ("0" + lDateObJ.getHours()).slice(-2) + ":" +
                          ("0" + lDateObJ.getMinutes()).slice(-2) + ":" +
                          ("0" + lDateObJ.getSeconds()).slice(-2);
  
      let lLogData = lLogFileTime + " " + pLineData + "\n";
  
      // Asynchronously append the log data
      await fs.promises.appendFile(filePath, lLogData, 'utf8');
    } catch (err) {
      console.error("Error writing log file:", err);
    }
}
  
},60000);
