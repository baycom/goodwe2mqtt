var util=require('util');
var mqtt=require('mqtt');
var ModbusRTU = require("modbus-serial");
var Parser = require('binary-parser').Parser;
const commandLineArgs = require('command-line-args')

const optionDefinitions = [
	{ name: 'mqtthost', alias: 'm', type: String, defaultValue: "localhost" },
	{ name: 'mqttclientid', alias: 'c', type: String, defaultValue: "goodwe1Client" },
	{ name: 'inverterhost', alias: 'i', type: String},
	{ name: 'inverterport', alias: 'p', type: String},
	{ name: 'type',         alias: 't', type: String, multiple: true, defaultValue: ['ET']}, 
        { name: 'address',      alias: 'a', type: Number, multiple: true, defaultValue: [1] },
        { name: 'wait',         alias: 'w', type: Number, defaultValue: 10000 },
        { name: 'debug',        alias: 'd', type: Boolean, defaultValue: false },
  ];

const options = commandLineArgs(optionDefinitions)

var GWSerialNumber=[];
var modbusClient = new ModbusRTU();

modbusClient.setTimeout(1000);

if(options.inverterhost) {
	modbusClient.connectTCP(options.inverterhost, { port: 502 }).catch((error) => {
		console.error(error);
		process.exit(-1);
	});
} else if(options.inverterport) {
	modbusClient.connectRTUBuffered(options.inverterport, { baudRate: 9600, parity: 'none' }).catch((error) => {
		console.error(error);
		process.exit(-1);
	});
}

console.log("MQTT Host         : " + options.mqtthost);
console.log("MQTT Client ID    : " + options.mqttclientid);

console.log("GoodWe MODBUS addr: " + options.address);
console.log("GoodWe Type       : " + options.type);

if(options.inverterhost) {
	console.log("GoodWe host       : " + options.inverterhost);
} else {
	console.log("GoodWe serial port: " + options.inverterport);
}

var MQTTclient = mqtt.connect("mqtt://"+options.mqtthost,{clientId: options.mqttclientid});
	MQTTclient.on("connect",function(){	
	console.log("MQTT connected");
})

MQTTclient.on("error",function(error){
		console.log("Can't connect" + error);
		process.exit(1)
	});

function sendMqtt(id, data) {
        if(options.debug) {
	        console.log("publish: "+'GoodWe/' + id, JSON.stringify(data));
	}
        MQTTclient.publish('GoodWe/' + id, JSON.stringify(data));        
}

const ETPayloadParser = new Parser()
	.seek((0x891F-0x891C)*2)
	.uint16be('PV1Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('PV1Current', { formatter: (x) => {return x/10.0;}})
	.uint32be('PV1Power')
	.uint16be('PV2Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('PV2Current', { formatter: (x) => {return x/10.0;}})
	.uint32be('PV2Power')
	.uint16be('PV3Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('PV3Current', { formatter: (x) => {return x/10.0;}})
	.uint32be('PV3Power')
	.uint16be('PV4Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('PV4Current', { formatter: (x) => {return x/10.0;}})
	.uint32be('PV4Power')
	.uint32be('PVWorkMode')
	.uint16be('OnGridL1Voltage', { formatter: (x) => {return x/10.0;}})
	.int16be('OnGridL1Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('OnGridL1Frequency', { formatter: (x) => {return x/100.0;}})
	.int32be('OnGridL1Power')
	.uint16be('OnGridL2Voltage', { formatter: (x) => {return x/10.0;}})
	.int16be('OnGridL2Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('OnGridL2Frequency', { formatter: (x) => {return x/100.0;}})
	.int32be('OnGridL2Power')
	.uint16be('OnGridL3Voltage', { formatter: (x) => {return x/10.0;}})
	.int16be('OnGridL3Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('OnGridL3Frequency', { formatter: (x) => {return x/100.0;}})
	.int32be('OnGridL3Power')
	.int16be('GridMode')
	.int32be('TotalInverterPower')
	.seek((0x8944-0x8943)*2)
	.int16be('ActivePower')
	.seek((0x8949-0x8945)*2)
	.uint16be('BackupL1Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('BackupL1Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('BackupL1Frequency', { formatter: (x) => {return x/100.0;}})
	.seek((0x894D-0x894C)*2)
	.int32be('BackupL1Power')
	.uint16be('BackupL2Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('BackupL2Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('BackupL2Frequency', { formatter: (x) => {return x/100.0;}})
	.seek((0x8953-0x8952)*2)
	.int32be('BackupL2Power')
	.uint16be('BackupL3Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('BackupL3Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('BackupL3Frequency', { formatter: (x) => {return x/100.0;}})
	.seek((0x8959-0x8958)*2)
	.int32be('BackupL3Power')
	.uint32be('LoadL1')
	.uint32be('LoadL2')
	.uint32be('LoadL3')
	.int32be('TotalBackupPower')
	.int32be('TotalLoadPower')
	.uint16be('UPSLoadPercent')
	.uint16be('AirTemperature', { formatter: (x) => {return x/10.0;}})
	.uint16be('ModuleTemperature', { formatter: (x) => {return x/10.0;}})
	.uint16be('RadiatorTemperature', { formatter: (x) => {return x/10.0;}})
	.uint16be('FunctionBitValue')
	.uint16be('BUSVoltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('NBUSVoltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('BatteryVoltage', { formatter: (x) => {return x/10.0;}})
	.int16be('BatteryCurrent', { formatter: (x) => {return x/10.0;}})
	.seek(2)
	.int16be('BatteryPower')
	.uint16be('BatteryMode')
	.uint16be('WarningCode')
	.uint16be('CountryCode')
	.uint16be('WorkMode')
	.uint16be('OperationMode')
	.uint32be('ErrorMessage')
	.uint32be('TotalPVGeneration', { formatter: (x) => {return x/10.0;}})
	.uint32be('TodayPVGeneration', { formatter: (x) => {return x/10.0;}})
	.uint32be('ETotal', { formatter: (x) => {return x/10.0;}})
	.uint32be('TotalHours')
	.uint16be('EDaySell', { formatter: (x) => {return x/10.0;}})
	.uint32be('ETotalBuy', { formatter: (x) => {return x/10.0;}})
	.uint16be('EDayBuy', { formatter: (x) => {return x/10.0;}})
	.uint32be('ETotalLoad', { formatter: (x) => {return x/10.0;}})
	.uint16be('ELoadDay', { formatter: (x) => {return x/10.0;}})
	.uint32be('EBatteryCharge', { formatter: (x) => {return x/10.0;}})
	.uint16be('EChargeDay', { formatter: (x) => {return x/10.0;}})
	.uint32be('EBatteryDischarge', { formatter: (x) => {return x/10.0;}})
	.uint16be('EDischargeDay', { formatter: (x) => {return x/10.0;}})
	.uint16be('BattStrings')
	.uint16be('CPLDWarningCode')
	.uint32be('wChargerCtrlFlg')
	.uint16be('DerateFlag')
	.uint32be('DerateFrozenPower')
	.uint32be('DiagStatusH')
	.uint32be('DiagStatusL')
	;
function getETPayload(data) {
	return ETPayloadParser.parse(data);
}


const getETSN = async (address) => {
	try {
		modbusClient.setID(address);
		let vals = await modbusClient.readHoldingRegisters(0x88BB, 8);
		GWSerialNumber[address] = new String(vals.buffer);
		if(options.debug) {
			console.log(GWSerialNumber);
		}
	} catch (e) {
		return null;
	}
}

const DTPayloadParser = new Parser()
	.uint16be('PV1Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('PV2Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('PV1Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('PV2Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('GridL1Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('GridL2Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('GridL3Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('GridL1Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('GridL2Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('GridL3Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('GridL1Frequency', { formatter: (x) => {return x/100.0;}})
	.uint16be('GridL2Frequency', { formatter: (x) => {return x/100.0;}})
	.uint16be('GridL3Frequency', { formatter: (x) => {return x/100.0;}})
	.uint16be('GridFeedingPowerL')
	.uint16be('WorkMode')
	.uint16be('Temperature', { formatter: (x) => {return x/10.0;}})
        .uint32be('ErrorMessage')
	.uint32be('ETotal', { formatter: (x) => {return x/10.0;}})
	.uint32be('HTotal', { formatter: (x) => {return x/10.0;}})
	.uint16be('Firmware')
	.uint16be('Warning')
	.uint16be('PV2FaultValue', { formatter: (x) => {return x/10.0;}})
	.uint16be('FunctionsValue')
	.uint16be('Line2VfaultValue', { formatter: (x) => {return x/10.0;}})
	.uint16be('Line3VfaultValue', { formatter: (x) => {return x/10.0;}})
	.uint16be('BUSVoltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('NBUSVoltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('Line3FfaultValue', { formatter: (x) => {return x/100.0;}})
	.uint16be('SafetyCountry')
	.uint16be('EDay', { formatter: (x) => {return x/10.0;}})
	;
function getDTPayload(data) {
	return DTPayloadParser.parse(data);
}

const getDTSN = async (address) => {
	try {
		modbusClient.setID(address);
		let vals = await modbusClient.readHoldingRegisters(0x200, 8);
		GWSerialNumber[address] = new String(vals.buffer);
		if(options.debug) {
			console.log(GWSerialNumber);
		}
	} catch (e) {
		return null;
	}
}

const getETRegisters = async (address) => {
	try {
		modbusClient.setID(address);
                let vals = await modbusClient.readHoldingRegisters(0x891C, 123);	
		var gwState = getETPayload(vals.buffer);
		if(options.debug) {
			console.log(util.inspect(gwState));
		}
		sendMqtt(GWSerialNumber[address], gwState);
	} catch (e) {
		return null;
	}
}

const getDTRegisters = async (address) => {
	try {
		modbusClient.setID(address);
                let vals = await modbusClient.readHoldingRegisters(0x300, 0x21);
		var gwState = getDTPayload(vals.buffer);
		if(options.debug) {
			console.log(util.inspect(gwState));
		}
		sendMqtt(GWSerialNumber[address], gwState);
	} catch (e) {
		return null;
	}
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const getMetersValue = async (meters) => {
    try{
        var pos=0;
        // get value of all meters
        for(let meter of meters) {
                if(options.debug) {
                        console.log("query: " + meter + " type: " + options.type[pos]);
                }
                if(!GWSerialNumber[meter]) {
			if(options.type[pos] == 'DT') {
				await getDTSN(meter);
			} else {
	                	await getETSN(meter);
			}
                }
                if(GWSerialNumber[meter]) {
                	if(options.type[pos] == 'DT') {
                		await getDTRegisters(meter);
                	} else {
				await getETRegisters(meter);
			}
		}
		pos++;
        }
	await sleep(options.wait);
    } catch(e){
        // if error, handle them here (it should not)
        console.log(e)
    } finally {
        // after get all data from salve repeate it again
        setImmediate(() => {
            getMetersValue(meters);
        })
    }
}

// start get value
getMetersValue(options.address);

