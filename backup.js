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
    var lTriggerRequiredHrs = "13:19";
    

    l_Current_Date_Time = l_Date_ObJ.getFullYear() + "-"+ (l_Date_ObJ.getMonth() + 1) + "-" + l_Date_ObJ.getDate() + " " + l_Date_ObJ.getHours() + ":" + l_Date_ObJ.getMinutes()
    var lTriggerDateTime = l_Date_ObJ.getFullYear() + "-"+ (l_Date_ObJ.getMonth() + 1) + "-" + l_Date_ObJ.getDate()+" "+ lTriggerRequiredHrs
    //F2FWriteToLogFile("Timer Tick TriggerHrs: " + lTriggerRequiredHrs + " Current Time: " + l_Current_Date_Time + " Trigger DateTime :" + lTriggerDateTime)
    F2FFarmerGetData()
    if(lTriggerDateTime==l_Current_Date_Time){
    //F2FWriteToLogFile(" Trigger Time Mached : " + lTriggerDateTime + "=" + l_Current_Date_Time)    
    //F2FFarmerGetData()
    
    }
    


async function F2FFarmerGetData(){

    try{
        
        await sql.connect(sqlConfig)
        var farmerrecordset = await sql.query(`
            WITH DeviceCTE AS (
                SELECT 
                    FO.Name AS FieldOfficer,
                    FO.MobileNumber1 AS FieldOfficerNumber,
                    FO.fEmail AS FOEmail,
                    DT.Name AS DeviceType,
                    FDD.DeviceEuIID,
                    Farmer.Name AS FarmerName,
                    Farmer.FatherName,
                    Farmer.MobileNumberPrimary,
                    Village.Name AS VillageName,
                    CONVERT(VARCHAR(20), TDS.CreateDate, 120) AS GatewayUpdatedTime,
                    RIGHT('0' + CAST(DATEDIFF(MINUTE, TDS.CreateDate, DATEADD(MINUTE, 330, GETUTCDATE())) / 60 AS VARCHAR), 2) 
                        + ':' +
                    RIGHT('0' + CAST(DATEDIFF(MINUTE, TDS.CreateDate, DATEADD(MINUTE, 330, GETUTCDATE())) % 60 AS VARCHAR), 2) + ' Hours' AS OfflineSinceFrom,
                    ROW_NUMBER() OVER (
                        PARTITION BY FO.Name, FO.MobileNumber1, FO.fEmail 
                        ORDER BY FDD.DeviceEuIID
                    ) AS SerialNo
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
                    AND FDD.DeviceTypeID IN (19)
                    AND FDD.FarmerSectionType = 'FL'
            )
        SELECT 
            FieldOfficer,
            FieldOfficerNumber,
            FOEmail,
            COUNT(DeviceEuIID) AS TotalDevicesDown,
            STRING_AGG(
                CONCAT(
                    SerialNo, '. ',
                    'DeviceType: ', DeviceType,
                    ', DeviceID: ', DeviceEuIID,
                    ', Farmer: ', FarmerName, ' S/O ', FatherName,
                    ', FarmerNumber: ', MobileNumberPrimary,
                    ', Village: ', VillageName,
                    ', GatewayUpdatedTime: ', GatewayUpdatedTime,
                    ', OfflineSinceFrom: ', OfflineSinceFrom
                ),
                '; '
            ) AS DeviceGroup
        FROM DeviceCTE
        GROUP BY 
            FieldOfficer, 
            FieldOfficerNumber, 
            FOEmail;
            `)
        
        var Pulled_record_of_farmer = (farmerrecordset.recordsets[0])
        var DgFarmerDetails = Pulled_record_of_farmer
        if(DgFarmerDetails.length==0){
            F2FWriteToLogFile("No Active Farmer Data Found - Exit Program")                
        }else{
            
                                                                                      
            
                
                //DgFarmerDetails                

                for(let i=0;i<DgFarmerDetails.length;i++){

                    console.log(DgFarmerDetails[i].DeviceGroup)
                    

                    // const messageBody = dedent(`
                    //     Dear ${DgFarmerDetails[i].FieldOfficer},
                    
                    //     Your Device Details:
                    //     ${DgFarmerDetails[i].DeviceGroup}
                    
                    //     Gateway offline immediate action required.
                    // `)

                    const deviceGroupFormatted = DgFarmerDetails[i].DeviceGroup.replace(/;\s*/g, ';\n');

                    const messageBody = dedent(`
                    Dear ${DgFarmerDetails[i].FieldOfficer},

                    Your Device Details:
                    ${deviceGroupFormatted}

                    Gateway offline immediate action required.
                    `);

                    F2FWriteToLogFile(messageBody)

                }
                
        }
        

    }
    catch(err){
        F2FWriteToLogFile(err)
    }

}



function F2FWriteToLogFile(pLineData) {   
        
  let lDateObJ = new Date();                
  var lDayWiseLogfile = 'LOGS' + lDateObJ.getFullYear() + "-"+ (lDateObJ.getMonth()+1) + "-" + lDateObJ.getDate() +'.txt'                
  const filePath = path.join(__dirname, 'LogsCB',lDayWiseLogfile); 
  var lLogFileTime =  ("0" + lDateObJ.getDate()).slice(-2) +"-"+ ("0" + (lDateObJ.getMonth() + 1)).slice(-2) + "-" +lDateObJ.getFullYear() +  " " + ("0" +lDateObJ.getHours()).slice(-2) + ":" + ("0" +lDateObJ.getMinutes()).slice(-2) + ":" + ("0" +lDateObJ.getSeconds()).slice(-2)
  var lLogData = lLogFileTime + " " + pLineData + "\n"
  fs.appendFileSync(filePath, lLogData, 'utf8');
}

},10000);
