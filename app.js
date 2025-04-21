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
    var lTriggerRequiredHrs = "9:0";
    

    l_Current_Date_Time = l_Date_ObJ.getFullYear() + "-"+ (l_Date_ObJ.getMonth() + 1) + "-" + l_Date_ObJ.getDate() + " " + l_Date_ObJ.getHours() + ":" + l_Date_ObJ.getMinutes()
    var lTriggerDateTime = l_Date_ObJ.getFullYear() + "-"+ (l_Date_ObJ.getMonth() + 1) + "-" + l_Date_ObJ.getDate()+" "+ lTriggerRequiredHrs
    var lTriggerDateTime1 = l_Date_ObJ.getFullYear() + "-"+ (l_Date_ObJ.getMonth() + 1) + "-" + l_Date_ObJ.getDate()+" "+ "11:0"
    var lTriggerDateTime2 = l_Date_ObJ.getFullYear() + "-"+ (l_Date_ObJ.getMonth() + 1) + "-" + l_Date_ObJ.getDate()+" "+ "13:0"
    var lTriggerDateTime3 = l_Date_ObJ.getFullYear() + "-"+ (l_Date_ObJ.getMonth() + 1) + "-" + l_Date_ObJ.getDate()+" "+ "15:0"
    var lTriggerDateTime4 = l_Date_ObJ.getFullYear() + "-"+ (l_Date_ObJ.getMonth() + 1) + "-" + l_Date_ObJ.getDate()+" "+ "17:0"

    
    
    if(lTriggerDateTime==l_Current_Date_Time||lTriggerDateTime1==l_Current_Date_Time||lTriggerDateTime2==l_Current_Date_Time||lTriggerDateTime3==l_Current_Date_Time||lTriggerDateTime4==l_Current_Date_Time){
    
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
    // 1) Split on semicolons, trim and drop any empty segments
    const segments = deviceGroupString
        .split(';')
        .map(s => s.trim())
        .filter(s => s.length > 0);
    
    const parsed = [];
    
    for (let seg of segments) {
        // 2) Extract the serial number by removing "<number>. " at the front
        const snoMatch = seg.match(/^(\d+)\.\s*/);
        const sno = snoMatch ? snoMatch[1] : '';
        // Remove that prefix from the text
        let body = snoMatch ? seg.slice(snoMatch[0].length) : seg;
    
        // 3) Now split into key:value pairs.
        //    We look for comma + (lookahead for word+colon) so we preserve values that have commas
        const pairs = body.split(/,\s*(?=[A-Za-z ]+:\s*)/);
    
        // 4) Build an object of the form { DeviceType: "...", Client: "...", ... }
        const obj = { sno };
        for (let pair of pairs) {
        let [key, val] = pair.split(/:\s*(.+)/); 
        if (!val) continue;     // skip if no colon
        key = key.trim();
        val = val.trim();
        obj[key] = val;
        }
    
        // 5) Push either the parsed object (if it has at least DeviceID) or raw fallback
        if (obj.DeviceID) {
        parsed.push({
            sno:                obj.sno,
            Client:             obj.Client || '',
            DeviceType:         obj.DeviceType || '',
            DeviceID:           obj.DeviceID || '',
            Farmer:             obj.Farmer || '',
            FarmerNumber:       obj.FarmerNumber || '',
            Village:            obj.Village || '',
            GatewayUpdatedTime: obj.GatewayUpdatedTime || '',
            OfflineSinceFrom:   obj.OfflineSinceFrom || ''
        });
        } else {
        parsed.push({ raw: seg });
        }
    }
    
    return parsed;
    }
         
    async function buildHtmlEmailFromDeviceGroup(fieldOfficerName, deviceGroupString) {
        const parsedRecords = await parseDeviceRecords(deviceGroupString);
      
        // 1) Extend the header row with a Client column
        const tableHeader = `
          <tr style="background: #f2f2f2;">
            <th style="border:1px solid #ccc; padding:5px; text-align:center;">S.No</th>
            <th style="border:1px solid #ccc; padding:5px; text-align:center;">Client</th>
            <th style="border:1px solid #ccc; padding:5px; text-align:center;">DeviceType</th>            
            <th style="border:1px solid #ccc; padding:5px; text-align:center;">DeviceID</th>
            <th style="border:1px solid #ccc; padding:5px; text-align:center;">Farmer</th>
            <th style="border:1px solid #ccc; padding:5px; text-align:center;">FarmerNumber</th>
            <th style="border:1px solid #ccc; padding:5px; text-align:center;">Village</th>
            <th style="border:1px solid #ccc; padding:5px; text-align:center;">DeviceLastcommunicated</th>
            <th style="border:1px solid #ccc; padding:5px; text-align:center;">OfflineSinceFrom</th>
          </tr>`;
      
        // 2) Include record.Client in each data row
        const tableRows = parsedRecords.map(r => {
          if (r.raw) {
            return `<tr><td colspan="9" style="border:1px solid #ccc; padding:5px; text-align:center;">${r.raw}</td></tr>`;
          }
          return `
            <tr>
              <td style="border:1px solid #ccc; padding:5px; text-align:center;">${r.sno}</td>
              <td style="border:1px solid #ccc; padding:5px; text-align:center;">${r.Client}</td>
              <td style="border:1px solid #ccc; padding:5px; text-align:center;">${r.DeviceType}</td>              
              <td style="border:1px solid #ccc; padding:5px; text-align:center;">${r.DeviceID}</td>
              <td style="border:1px solid #ccc; padding:5px; text-align:center;">${r.Farmer}</td>
              <td style="border:1px solid #ccc; padding:5px; text-align:center;">${r.FarmerNumber}</td>
              <td style="border:1px solid #ccc; padding:5px; text-align:center;">${r.Village}</td>
              <td style="border:1px solid #ccc; padding:5px; text-align:center;">${r.GatewayUpdatedTime}</td>
              <td style="border:1px solid #ccc; padding:5px; text-align:center;">${r.OfflineSinceFrom}</td>
            </tr>`;
        }).join('');
      
        const tableHtml = `
          <table style="border-collapse:collapse; width:100%; font-family:Arial,sans-serif; font-size:14px; text-align:center;">
            <thead>${tableHeader}</thead>
            <tbody>${tableRows}</tbody>
          </table>`;
      
        return dedent(`
          <p>Dear ${fieldOfficerName},</p>
          <p>Your Offline Device Details are as follows:</p>
          ${tableHtml}
          <p>Devices offline immediate action required.</p>
          <p>Regards,<br/>Team CultYvate</p>
          <p>This is an automated email; please do not reply.</p>
          <p><a href="https://www.cultyvate.com/" target="_blank">https://www.cultyvate.com/</a></p>
        `);
    }
      
      
async function F2FFarmerGetData(){

    try{
        
        await sql.connect(sqlConfig)
        var query = `
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

            `
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
					Client.Name as Client,
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
					INNER JOIN Client 
						ON Farmer.ClientID = Client.ID
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
                    Farmer.ClientID  IN (7,11,23,24,25,26,29,36,37,39,48,54,55,58,59)                    
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
					Client.Name as Client,
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
					INNER JOIN Client 
						ON Farmer.ClientID = Client.ID
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
                    Farmer.ClientID  IN (7,11,23,24,25,26,29,36,37,39,48,54,55,58,59)                    
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
					', Client: ',Client,
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
                    

                    //cc:['Abhinav@cultyvate.com','anil@cultyvate','praveenkumar.cultyvate@gmail.com'],
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

async function F2FWriteToLogFile(pLineData) {   
    try {
      let lDateObJ = new Date();
      // Build the logfile name
      let lDayWiseLogfile = 'LOGS' + lDateObJ.getFullYear() + "-" + (lDateObJ.getMonth() + 1) + "-" + lDateObJ.getDate() + '.txt';
  
      // Use path.resolve to define the log folder robustly.
      // Change '..' to adjust the folder level if required.
      const logsFolder = path.resolve(__dirname, 'EmailLogs');
  
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
