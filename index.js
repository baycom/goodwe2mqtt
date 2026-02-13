const util = require('util');
const Mutex = require('async-mutex').Mutex;
const mqtt = require('mqtt');
const ModbusRTU = require("modbus-serial");
const Parser = require('binary-parser').Parser;
const commandLineArgs = require('command-line-args')

const networkErrors = ["ESOCKETTIMEDOUT", "ECONNRESET", "ECONNREFUSED", "EHOSTUNREACH", "ETIMEDOUT"];

const optionDefinitions = [
	{ name: 'mqtthost', alias: 'm', type: String, defaultValue: "localhost" },
	{ name: 'mqttclientid', alias: 'c', type: String, defaultValue: "gwClient" },
	{ name: 'inverterhost', alias: 'i', type: String },
	{ name: 'inverterport', alias: 'p', type: String },
	{ name: 'type', alias: 't', type: String, multiple: true, defaultValue: ['ET'] },
	{ name: 'address', alias: 'a', type: Number, multiple: true, defaultValue: [1] },
	{ name: 'wait', alias: 'w', type: Number, defaultValue: 10000 },
	{ name: 'debug', alias: 'd', type: Boolean, defaultValue: false },
	{ name: 'na', alias: 'n', type: String, defaultValue: ['NA/0/state'] }
];

const options = commandLineArgs(optionDefinitions)

var GWSerialNumber = [];
var modbusClient = new ModbusRTU();
var mutex = new Mutex();

modbusClient.setTimeout(1000);

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

if (options.inverterhost) {
	modbusClient.connectTcpRTUBuffered(options.inverterhost, { port: 502 }).then(val => {
		// start get value
		getStatus();
	}).catch((error) => {
		console.error("connectTcpRTUBuffered: " + error.message);
		process.exit(-1);
	});
} else if (options.inverterport) {
	modbusClient.connectRTUBuffered(options.inverterport, { baudRate: 9600, parity: 'none' }).then((val) => {
		// start get value
		getStatus();
	}).catch((error) => {
		console.error("connectRTUBuffered: " + error.message);
		process.exit(-1);
	});
}

console.log("MQTT Host         : " + options.mqtthost);

console.log("GoodWe MODBUS addr: " + options.address);
console.log("GoodWe Type       : " + options.type);
console.log("NA                : " + options.na);

if (options.inverterhost) {
	console.log("GoodWe host       : " + options.inverterhost);
} else {
	console.log("GoodWe serial port: " + options.inverterport);
}

var MQTTclient = mqtt.connect("mqtt://" + options.mqtthost);
MQTTclient.on("connect", function () {
	console.log("MQTT connected");
	MQTTclient.subscribe("GoodWe/+/+/set");
	if (options.na) {
		MQTTclient.subscribe(options.na);
	}
})

MQTTclient.on("error", function (error) {
	console.log("Can't connect" + error);
	process.exit(1)
});

function sendMqtt(address, data) {
	if (options.debug) {
		console.log("publish: " + 'GoodWe/' + address, JSON.stringify(data));
	}
	MQTTclient.publish('GoodWe/' + address, JSON.stringify(data), { retain: true });
}

function findModbusAddr(serial) {
	var pos = 0;
	for (let address of options.address) {
		if (options.debug) {
			console.log("query: " + address + " type: " + options.type[pos]);
		}
		if (GWSerialNumber[address] == serial) {
			if (options.debug) {
				console.log("found modbus address: ", address);
			}
			return address;
		}
		pos++;
	}
	if (options.debug) {
		console.log("modbus address not found for serial:", serial);
	}
	return -1;
}

function findType(serial) {
	var pos = 0;
	for (let address of options.address) {
		if (options.debug) {
			console.log("query: " + address + " type: " + options.type[pos]);
		}
		if (GWSerialNumber[address] == serial) {
			if (options.debug) {
				console.log("found modbus address: ", address);
			}
			return options.type[pos];
		}
		pos++;
	}
	if (options.debug) {
		console.log("modbus address not found for serial:", serial);
	}
	return undefined;
}

async function modbusWrite(serial, func, reg, value, query = 0) {
	var addr = findModbusAddr(serial);
	if (addr > 0) {
		return await mutex.runExclusive(async () => {
			try {
				modbusClient.setID(addr);
				var ret;
				if (!query) {
					await modbusClient.writeRegister(reg, value);
					MQTTclient.publish('GoodWe/' + serial + "/" + func + "/result", value.toString());
				} else {
					ret = await modbusClient.readHoldingRegisters(reg, 1);
					MQTTclient.publish('GoodWe/' + serial + "/" + func + "/result", ret.buffer.readUInt16BE(0).toString());
				}
				return ret;
			} catch (e) {
				MQTTclient.publish('GoodWe/' + serial + "/" + func + "/result", "failed: " + e.message);
				console.error("modbusWrite: " + e.message);
			}
		});
	}
	return -1;
}

MQTTclient.on('message', function (topic, message, packet) {
	if (options.debug) {
		console.log("MQTT message for topic ", topic, " received: ", message);
	}
	if (topic.includes("GoodWe/")) {
		let sub = topic.split('/');
		let serial = sub[1];
		let func = sub[2];
		let value = parseInt(message);
		let query = message.length == 0
		let register = -1;
		let type = findType(serial);
		
		if(type === 'ET') {
			if (func === 'socminongrid') {
				register = 45356;
			} else if (func === 'socminoffgrid') {
				register = 45358;
			} else if (func === 'chargeforcegrid') {
				register = 47545;
			} else if (func === 'chargeforcesoc') {
				register = 47546;
			} else if (func === 'chargeforcepower') {
				register = 47603;
			} else if (func === 'rapidcutoff') {
				register = 45255;
			}
		} else if(type === 'DT') {
			if (func === 'rapidcutoff') {
				if(value == 0) {
					modbusWrite(GWSerialNumber[address], 'rapidcutoff', 120, 0);
				} else {
					modbusWrite(GWSerialNumber[address], 'rapidcutoff', 121, 0);
				}	
			}
		}
		if (register != -1) {
			modbusWrite(serial, func, register, value, query);
		}
	} else if ((index = options.na.indexOf(topic)) >= 0) {
		let val = JSON.parse(message);
		if (options.debug) {
			console.log("NA: ", options.na, " state: ", val);
		}
		var pos = 0;
		for (let address of options.address) {
			if (options.debug) {
				console.log("query: " + address + " type: " + options.type[pos]);
			}
			if (options.type[pos] == 'ET') {
				if (options.debug) {
					console.log("found modbus address: ", address);
				}
				modbusWrite(GWSerialNumber[address], 'rapidcutoff', 45255, val == 0 ? 1 : 0);
			} else {
				if(val == 0) {
					modbusWrite(GWSerialNumber[address], 'rapidcutoff', 120, 0);
				} else {
					modbusWrite(GWSerialNumber[address], 'rapidcutoff', 121, 0);
				}
			}
			pos++;
		}
	}
});

const ETPayloadParser_35001 = new Parser()
	.uint16('RatePower')
	.seek(2)
	.string('INVSN', { length: 16 })
	.string('ModelName', { length: 10 })
	.uint16('FMVersionDSPM')
	.uint16('FMVersionDSPS')
	.uint16('BetaVersionDSP')
	.uint16('FMVersionARM')
	.uint16('BetaVersionARM')
	.uint16('FMVersionDSPDCDC')
	.uint16('BetaVersionDCDC')
	.uint16('FMVersionDSPMPPT')
	.uint16('BetaVersionMPPT')
	.uint16('FMVersionDSPSTS')
	.uint16('BetaVersionSTS')
	;

const ETPayloadParser_35100 = new Parser()
	.seek((35103 - 35100) * 2)
	.uint16be('PV1Voltage', { formatter: (x) => { return x != 65535 ? (x / 10.0) : 0; } })
	.uint16be('PV1Current', { formatter: (x) => { return x != 65535 ? (x / 10.0) : 0; } })
	.uint32be('PV1Power')
	.uint16be('PV2Voltage', { formatter: (x) => { return x != 65535 ? (x / 10.0) : 0; } })
	.uint16be('PV2Current', { formatter: (x) => { return x != 65535 ? (x / 10.0) : 0; } })
	.uint32be('PV2Power')
	.uint16be('PV3Voltage', { formatter: (x) => { return x != 65535 ? (x / 10.0) : 0; } })
	.uint16be('PV3Current', { formatter: (x) => { return x != 65535 ? (x / 10.0) : 0; } })
	.uint32be('PV3Power')
	.uint16be('PV4Voltage', { formatter: (x) => { return x != 65535 ? (x / 10.0) : 0; } })
	.uint16be('PV4Current', { formatter: (x) => { return x != 65535 ? (x / 10.0) : 0; } })
	.uint32be('PV4Power')
	.uint32be('PVWorkMode')
	.uint16be('OnGridL1Voltage', { formatter: (x) => { return x / 10.0; } })
	.int16be('OnGridL1Current', { formatter: (x) => { return x / 10.0; } })
	.uint16be('OnGridL1Frequency', { formatter: (x) => { return x / 100.0; } })
	.int32be('OnGridL1Power')
	.uint16be('OnGridL2Voltage', { formatter: (x) => { return x / 10.0; } })
	.int16be('OnGridL2Current', { formatter: (x) => { return x / 10.0; } })
	.uint16be('OnGridL2Frequency', { formatter: (x) => { return x / 100.0; } })
	.int32be('OnGridL2Power')
	.uint16be('OnGridL3Voltage', { formatter: (x) => { return x / 10.0; } })
	.int16be('OnGridL3Current', { formatter: (x) => { return x / 10.0; } })
	.uint16be('OnGridL3Frequency', { formatter: (x) => { return x / 100.0; } })
	.int32be('OnGridL3Power')
	.int16be('GridMode')
	.int32be('TotalInverterPower')
	.int32be('ActivePower')
	.int32be('ReactivePower')
	.int32be('ApparentPower')
	.uint16be('BackupL1Voltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('BackupL1Current', { formatter: (x) => { return x / 10.0; } })
	.uint16be('BackupL1Frequency', { formatter: (x) => { return x / 100.0; } })
	.seek((35149 - 35148) * 2)
	.int32be('BackupL1Power')
	.uint16be('BackupL2Voltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('BackupL2Current', { formatter: (x) => { return x / 10.0; } })
	.uint16be('BackupL2Frequency', { formatter: (x) => { return x / 100.0; } })
	.seek((35155 - 35154) * 2)
	.int32be('BackupL2Power')
	.uint16be('BackupL3Voltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('BackupL3Current', { formatter: (x) => { return x / 10.0; } })
	.uint16be('BackupL3Frequency', { formatter: (x) => { return x / 100.0; } })
	.seek((35161 - 35160) * 2)
	.int32be('BackupL3Power')
	.uint32be('LoadL1')
	.uint32be('LoadL2')
	.uint32be('LoadL3')
	.int32be('TotalBackupPower')
	.int32be('TotalLoadPower')
	.uint16be('UPSLoadPercent')
	.uint16be('AirTemperature', { formatter: (x) => { return x / 10.0; } })
	.uint16be('ModuleTemperature', { formatter: (x) => { return x / 10.0; } })
	.uint16be('RadiatorTemperature', { formatter: (x) => { return x / 10.0; } })
	.uint16be('FunctionBitValue')
	.uint16be('BUSVoltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('NBUSVoltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('BatteryVoltage', { formatter: (x) => { return x / 10.0; } })
	.int16be('BatteryCurrent', { formatter: (x) => { return x / 10.0; } })
	.int32be('BatteryPower')
	.uint16be('BatteryMode')
	.uint16be('WarningCode')
	.uint16be('CountryCode')
	.uint16be('WorkMode')
	.uint16be('OperationMode')
	.uint32be('ErrorMessage')
	.uint32be('TotalPVGeneration', { formatter: (x) => { return x / 10.0; } })
	.uint32be('TodayPVGeneration', { formatter: (x) => { return x / 10.0; } })
	.uint32be('ETotal', { formatter: (x) => { return x / 10.0; } })
	.uint32be('TotalHours')
	.uint16be('EDaySell', { formatter: (x) => { return x / 10.0; } })
	.uint32be('ETotalBuy', { formatter: (x) => { return x / 10.0; } })
	.uint16be('EDayBuy', { formatter: (x) => { return x / 10.0; } })
	.uint32be('ETotalLoad', { formatter: (x) => { return x / 10.0; } })
	.uint16be('ELoadDay', { formatter: (x) => { return x / 10.0; } })
	.uint32be('EBatteryCharge', { formatter: (x) => { return x / 10.0; } })
	.uint16be('EChargeDay', { formatter: (x) => { return x / 10.0; } })
	.uint32be('EBatteryDischarge', { formatter: (x) => { return x / 10.0; } })
	.uint16be('EDischargeDay', { formatter: (x) => { return x / 10.0; } })
	.uint16be('BattStrings')
	.uint16be('CPLDWarningCode')
	.uint32be('wChargerCtrlFlg')
	.uint16be('DerateFlag')
	.uint32be('DerateFrozenPower')
	.uint32be('DiagStatusH')
	.uint32be('DiagStatusL')
	;

const ETPayloadParser_35304 = new Parser()
	.uint16be('PV5Voltage', { formatter: (x) => { return x != 65535 ? (x / 10.0) : 0; } })
	.uint16be('PV5Current', { formatter: (x) => { return x != 65535 ? (x / 10.0) : 0; } })
	.uint16be('PV6Voltage', { formatter: (x) => { return x != 65535 ? (x / 10.0) : 0; } })
	.uint16be('PV6Current', { formatter: (x) => { return x != 65535 ? (x / 10.0) : 0; } })
	.seek((35336 - 35307) * 2)
	.uint16be('MPPT1Power', { formatter: (x) => { return x != 65535 ? x : -1; } })
	.uint16be('MPPT2Power', { formatter: (x) => { return x != 65535 ? x : -1; } })
	.uint16be('MPPT3Power', { formatter: (x) => { return x != 65535 ? x : -1; } })
	.seek((35344 - 35339) * 2)
	.uint16be('MPPT1Current', { formatter: (x) => { return x != 65535 ? (x / 10.0) : -1; } })
	.uint16be('MPPT2Current', { formatter: (x) => { return x != 65535 ? (x / 10.0) : -1; } })
	.uint16be('MPPT3Current', { formatter: (x) => { return x != 65535 ? (x / 10.0) : -1; } })
	;

const ETPayloadParser_36003 = new Parser()
	.uint16be('bMeterConnectStatus')
	.uint16be('MeterCommunicationStatus')
	.int16be('MTActivePowerL1')
	.int16be('MTActivePowerL2')
	.int16be('MTActivePowerL3')
	.int16be('MTTotalActivePower')
	.int16be('MTTotalReactivePower')
	.int16be('MeterPFL1', { formatter: (x) => { return x / 100.0; } })
	.int16be('MeterPFL2', { formatter: (x) => { return x / 100.0; } })
	.int16be('MeterPFL3', { formatter: (x) => { return x / 100.0; } })
	.int16be('MeterPowerFactor', { formatter: (x) => { return x / 100.0; } })
	.uint16be('MeterFrequency', { formatter: (x) => { return x / 100.0; } })
	.floatbe('MeterETotalSell')
	.floatbe('MeterETotalBuy')
	.int32be('MeterActivePowerL1')
	.int32be('MeterActivePowerL2')
	.int32be('MeterActivePowerL3')
	.int32be('MeterTotalActivePower')
	.int32be('MeterReactivePowerL1')
	.int32be('MeterReactivePowerL2')
	.int32be('MeterReactivePowerL3')
	.int32be('MeterTotalReactivePower')
	.int32be('MeterApparentPowerL1')
	.int32be('MeterApparentPowerL2')
	.int32be('MeterApparentPowerL3')
	.int32be('MeterTotalApparentPower')
	.uint16be('MeterType')
	.uint16be('MeterSoftwareVersion')
	.int32be('MeterCT2ActivePower')
	.uint32be('CT2ETotalSell', { formatter: (x) => { return x / 100.0; } })
	.uint32be('CT2ETotalBuy', { formatter: (x) => { return x / 100.0; } })
	.uint16be('MeterCT2status')
	.uint16be('MeterVoltageL1', { formatter: (x) => { return x / 10.0; } })
	.uint16be('MeterVoltageL2', { formatter: (x) => { return x / 10.0; } })
	.uint16be('MeterVoltageL3', { formatter: (x) => { return x / 10.0; } })
	.uint16be('MeterCurrentL1', { formatter: (x) => { return x / 10.0; } })
	.uint16be('MeterCurrentL2', { formatter: (x) => { return x / 10.0; } })
	.uint16be('MeterCurrentL3', { formatter: (x) => { return x / 10.0; } })
	.seek((36065 - 36058) * 2)
	.uint16be('ARCFaultChannel')
	.uint16be('EzloggerProCommStatus')
	.uint16be('ARCSoftwareVersion')
	;

const ETPayloadParser_36092 = new Parser()
	.uint64be('ActiveEtotalSellL1', { formatter: (x) => { return parseFloat(x) / 100.0; } })
	.uint64be('ActiveEtotalSellL2', { formatter: (x) => { return parseFloat(x) / 100.0; } })
	.uint64be('ActiveEtotalSellL3', { formatter: (x) => { return parseFloat(x) / 100.0; } })
	.uint64be('ActiveEtotalSellTotal', { formatter: (x) => { return parseFloat(x) / 100.0; } })
	.uint64be('ActiveEtotalBuyL1', { formatter: (x) => { return parseFloat(x) / 100.0; } })
	.uint64be('ActiveEtotalBuyL2', { formatter: (x) => { return parseFloat(x) / 100.0; } })
	.uint64be('ActiveEtotalBuyL3', { formatter: (x) => { return parseFloat(x) / 100.0; } })
	.uint64be('ActiveEtotalBuyTotal', { formatter: (x) => { return parseFloat(x) / 100.0; } })
	.uint16be('RealTimeClockYearMonth')
	.uint16be('RealTimeClockDayHour')
	.uint16be('RealTimeClockMinuteSecond')
	;

const ETPayloadParser_37000 = new Parser()
	.uint16be('DRMStatus')
	.uint16be('BattTypeIndex')
	.uint16be('BMSStatus')
	.uint16be('BMSPackTemperature', { formatter: (x) => { return x / 10.0; } })
	.uint16be('BMSChargeImax')
	.uint16be('BMSDischargeImax')
	.uint16be('BMSErrorCodeL')
	.uint16be('SOC')
	.uint16be('BMSSOH')
	.uint16be('BMSBatteryStrings')
	.uint16be('BMSWarningCodeL')
	.uint16be('BatteryProtocol')
	.uint16be('BMSErrorCodeH')
	.uint16be('BMSWarningCodeH')
	.uint16be('BMSSoftwareVersion')
	.uint16be('BatteryHardwareVersion')
	.uint16be('MaximumCellTemperatureID')
	.uint16be('MinimumCellTemperatureID')
	.uint16be('MaximumCellVoltageID')
	.uint16be('MinimumCellVoltageID')
	.uint16be('MaximumCellTemperature', { formatter: (x) => { return x / 10.0; } })
	.uint16be('MinimumCellTemperature', { formatter: (x) => { return x / 10.0; } })
	.uint16be('MaximumCellVoltage', { formatter: (x) => { return x / 1000.0; } })
	.uint16be('MinimumCellVoltage', { formatter: (x) => { return x / 1000.0; } })
	;

const ETPayloadParser_39000 = new Parser()
	.uint16be('BMS2Status')
	.uint16be('BMS2PackTemperature', { formatter: (x) => { return x / 10.0; } })
	.uint16be('BMS2ChargeImax')
	.uint16be('BMS2DischargeImax')
	.uint16be('BMS2ErrorCodeL')
	.uint16be('BMS2SOC')
	.uint16be('BMS2SOH')
	.uint16be('BMS2BatteryStrings')
	.uint16be('BMS2WarningCodeL')
	.uint16be('Battery2Protocol')
	.uint16be('BMS2ErrorCodeH')
	.uint16be('BMS2WarningCodeH')
	.uint16be('BMS2SoftwareVersion')
	.uint16be('Battery2HardwareVersion')
	.uint16be('BMS2MaximumCellTemperatureID')
	.uint16be('BMS2MinimumCellTemperatureID')
	.uint16be('BMS2MaximumCellVoltageID')
	.uint16be('BMS2MinimumCellVoltageID')
	.uint16be('BMS2MaximumCellTemperature', { formatter: (x) => { return x / 10.0; } })
	.uint16be('BMS2MinimumCellTemperature', { formatter: (x) => { return x / 10.0; } })
	.uint16be('BMS2MaximumCellVoltage', { formatter: (x) => { return x / 1000.0; } })
	.uint16be('BMS2MinimumCellVoltage', { formatter: (x) => { return x / 1000.0; } })
	;

const ETPayloadParser_45222 = new Parser()
	.uint32be('TotalPVGeneration', { formatter: (x) => { return x / 10.0; } })
	.uint32be('TodayPVGeneration', { formatter: (x) => { return x / 10.0; } })
	.uint32be('ETotalSell', { formatter: (x) => { return x / 10.0; } })
	.uint32be('TotalHours')
	.uint16be('EDaySell', { formatter: (x) => { return x / 10.0; } })
	.uint32be('ETotalBuy', { formatter: (x) => { return x / 10.0; } })
	.uint16be('EDayBuy', { formatter: (x) => { return x / 10.0; } })
	.uint32be('ETotalLoad', { formatter: (x) => { return x / 10.0; } })
	.uint16be('ELoadDay', { formatter: (x) => { return x / 10.0; } })
	.uint32be('EBatteryCharge', { formatter: (x) => { return x / 10.0; } })
	.uint16be('EChargeDay', { formatter: (x) => { return x / 10.0; } })
	.uint32be('EBatteryDischarge', { formatter: (x) => { return x / 10.0; } })
	.uint16be('EDischargeDay', { formatter: (x) => { return x / 10.0; } })
	;

const ETPayloadParser_35262 = new Parser()
	.uint16be('Battery2Voltage', { formatter: (x) => { return x / 10.0; } })
	.int16be('Battery2Current', { formatter: (x) => { return x / 10.0; } })
	.int32be('Battery2Power')
	.uint16be('Battery2Mode')
	;

const getETRegisters = async (address) => {
	try {
		modbusClient.setID(address);
		if (options.debug) {
			console.log("35001");
		}
		let vals = await modbusClient.readHoldingRegisters(35001, 40);
		var gwState_35001 = ETPayloadParser_35001.parse(vals.buffer);
		if (options.debug) {
			console.log("35100");
		}
		await sleep(100);
		vals = await modbusClient.readHoldingRegisters(35100, 123);
		var gwState_35100 = ETPayloadParser_35100.parse(vals.buffer);
		if (options.debug) {
			console.log("35304");
		}
		await sleep(100);
		vals = await modbusClient.readHoldingRegisters(35304, 44);
		var gwState_35304 = ETPayloadParser_35304.parse(vals.buffer);
		if (options.debug) {
			console.log("36003");
		}
		await sleep(100);
		vals = await modbusClient.readHoldingRegisters(36003, 65);
		var gwState_36003 = ETPayloadParser_36003.parse(vals.buffer);
		if (gwState_35001.RatePower >= 15000) {
			if (options.debug) {
				console.log("36092");
			}
			await sleep(100);
			vals = await modbusClient.readHoldingRegisters(36092, 35);
			var gwState_36092 = ETPayloadParser_36092.parse(vals.buffer);

			if (options.debug) {
				console.log("39000");
			}
			await sleep(100);
			vals = await modbusClient.readHoldingRegisters(39000, 48);
			var gwState_39000 = ETPayloadParser_39000.parse(vals.buffer);
		}
		if (options.debug) {
			console.log("37000");
		}
		await sleep(100);
		vals = await modbusClient.readHoldingRegisters(37000, 48);
		var gwState_37000 = ETPayloadParser_37000.parse(vals.buffer);

		if (options.debug) {
			console.log("45222");
		}
		await sleep(100);
		vals = await modbusClient.readHoldingRegisters(45222, 22);
		var gwState_45222 = ETPayloadParser_45222.parse(vals.buffer);

		if (options.debug) {
			console.log("47924");
		}
		await sleep(100);
		vals = await modbusClient.readHoldingRegisters(35262, 6);
		var gwState_35262 = ETPayloadParser_35262.parse(vals.buffer);

		var gwState = {};
		Object.assign(gwState, gwState_35100, gwState_35262, gwState_35304, gwState_36003, gwState_36092, gwState_37000, gwState_39000, gwState_45222);

		gwState.PV1Power = parseInt(gwState.PV1Voltage * gwState.PV1Current);
		gwState.PV2Power = parseInt(gwState.PV2Voltage * gwState.PV2Current);
		gwState.PV3Power = parseInt(gwState.PV3Voltage * gwState.PV3Current);
		gwState.PV4Power = parseInt(gwState.PV4Voltage * gwState.PV4Current);
		gwState.PV5Power = parseInt(gwState.PV5Voltage * gwState.PV5Current);
		gwState.PV6Power = parseInt(gwState.PV6Voltage * gwState.PV6Current);
		GWSerialNumber[address] = gwState_35001.INVSN;

		await sendMqtt(GWSerialNumber[address], gwState);
		if (options.debug) {
			console.log(util.inspect(gwState_35001));
			console.log(util.inspect(gwState));
		}
		return gwState;
	} catch (e) {
		if (options.debug) {
			console.error("getETRegisters: ", e.message, " errno: ", e.errno);
		}
		if (e.errno) {
			if (networkErrors.includes(e.errno)) {
				process.exit(-1);
			}
		}
		return null;
	}
}

const DTPayloadParser = new Parser()
	.uint16be('PV1Voltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('PV2Voltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('PV1Current', { formatter: (x) => { return x / 10.0; } })
	.uint16be('PV2Current', { formatter: (x) => { return x / 10.0; } })
	.uint16be('GridL1Voltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('GridL2Voltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('GridL3Voltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('GridL1Current', { formatter: (x) => { return x / 10.0; } })
	.uint16be('GridL2Current', { formatter: (x) => { return x / 10.0; } })
	.uint16be('GridL3Current', { formatter: (x) => { return x / 10.0; } })
	.uint16be('GridL1Frequency', { formatter: (x) => { return x / 100.0; } })
	.uint16be('GridL2Frequency', { formatter: (x) => { return x / 100.0; } })
	.uint16be('GridL3Frequency', { formatter: (x) => { return x / 100.0; } })
	.uint16be('GridFeedingPowerL')
	.uint16be('WorkMode')
	.uint16be('Temperature', { formatter: (x) => { return x / 10.0; } })
	.uint32be('ErrorMessage')
	.uint32be('ETotal', { formatter: (x) => { return x / 10.0; } })
	.uint32be('HTotal', { formatter: (x) => { return x / 10.0; } })
	.uint16be('Firmware')
	.uint16be('Warning')
	.uint16be('PV2FaultValue', { formatter: (x) => { return x / 10.0; } })
	.uint16be('FunctionsValue')
	.uint16be('Line2VfaultValue', { formatter: (x) => { return x / 10.0; } })
	.uint16be('Line3VfaultValue', { formatter: (x) => { return x / 10.0; } })
	.uint16be('BUSVoltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('NBUSVoltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('Line3FfaultValue', { formatter: (x) => { return x / 100.0; } })
	.uint16be('SafetyCountry')
	.uint16be('EDay', { formatter: (x) => { return x / 10.0; } })
	;

async function getDTRegisters(address) {
	try {
		modbusClient.setID(address);
		if (GWSerialNumber[address] === undefined) {
			let vals = await modbusClient.readHoldingRegisters(0x200, 8);
			var SNStr = new String(vals.buffer);
			GWSerialNumber[address] = SNStr;
		}
		await sleep(50);
		vals = await modbusClient.readHoldingRegisters(0x300, 0x21);
		var gwState = DTPayloadParser.parse(vals.buffer);
		gwState.PV1Power = parseInt(gwState.PV1Voltage * gwState.PV1Current);
		gwState.PV2Power = parseInt(gwState.PV2Voltage * gwState.PV2Current);
		if(gwState.PV1Voltage < 1200 && gwState.PV2Voltage < 1200 && gwState.PV1Current < 120 && gwState.PV2Current < 120 ) {
			await sendMqtt(GWSerialNumber[address], gwState);
			if (options.debug) {
				console.log(util.inspect(gwState));
			}
		}
		return gwState;
	} catch (e) {
		if (options.debug) {
			console.error("getDTRegisters: " + e.message);
		}
		if (e.errno) {
			if (networkErrors.includes(e.errno)) {
				process.exit(-1);
			}
		}
		return null;
	}
}


async function getStatus() {
	try {
		var pos = 0;
		// get value of all addresss
		for (let address of options.address) {
			let type = options.type[pos];
			if (options.debug) {
				console.log("query: " + address + " type: " + type);
			}
			await mutex.runExclusive(async () => {
				if (type == 'DT') {
					await getDTRegisters(address);
				} else {
					await getETRegisters(address);
				}
			});
			pos++;
		}
		await sleep(options.wait);
	} catch (e) {
		// if error, handle them here (it should not)
		console.error("getStatus: " + e.message)
	} finally {
		// after get all data from salve repeate it again
		setImmediate(() => {
			getStatus();
		})
	}
}
