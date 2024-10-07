const util = require('util');
const Mutex = require('async-mutex').Mutex;
const mqtt = require('mqtt');
const ModbusRTU = require("modbus-serial");
const Parser = require('binary-parser').Parser;
const commandLineArgs = require('command-line-args')

const networkErrors = ["ESOCKETTIMEDOUT", "ETIMEDOUT", "ECONNRESET", "ECONNREFUSED", "EHOSTUNREACH"];

const optionDefinitions = [
	{ name: 'mqtthost', alias: 'm', type: String, defaultValue: "localhost" },
	{ name: 'mqttclientid', alias: 'c', type: String, defaultValue: "gwClient" },
	{ name: 'inverterhost', alias: 'i', type: String },
	{ name: 'inverterport', alias: 'p', type: String },
	{ name: 'type', alias: 't', type: String, multiple: true, defaultValue: ['ET'] },
	{ name: 'address', alias: 'a', type: Number, multiple: true, defaultValue: [1] },
	{ name: 'wait', alias: 'w', type: Number, defaultValue: 10000 },
	{ name: 'debug', alias: 'd', type: Boolean, defaultValue: false },
];

const options = commandLineArgs(optionDefinitions)

var GWSerialNumber = [];
var modbusClient = new ModbusRTU();
var mutex = new Mutex();

modbusClient.setTimeout(1000);

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

if (options.inverterhost) {
	console.log("GoodWe host       : " + options.inverterhost);
} else {
	console.log("GoodWe serial port: " + options.inverterport);
}

var MQTTclient = mqtt.connect("mqtt://" + options.mqtthost);
MQTTclient.on("connect", function () {
	console.log("MQTT connected");
	MQTTclient.subscribe("GoodWe/+/+/set");
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
		if (options.type[pos] == 'ET' && GWSerialNumber[address] == serial) {
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
		let query = message.length==0
		let register = -1;
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
		} else if (func === 'rapaidcutoff') {
			register = 45255;
		}
		if(register != -1) {
			modbusWrite(serial, func, register, value, query);
		}
	}
});

const ETPayloadParser_35100 = new Parser()
	.seek((35103 - 35100) * 2)
	.uint16be('PV1Voltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('PV1Current', { formatter: (x) => { return x / 10.0; } })
	.uint32be('PV1Power')
	.uint16be('PV2Voltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('PV2Current', { formatter: (x) => { return x / 10.0; } })
	.uint32be('PV2Power')
	.uint16be('PV3Voltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('PV3Current', { formatter: (x) => { return x / 10.0; } })
	.uint32be('PV3Power')
	.uint16be('PV4Voltage', { formatter: (x) => { return x / 10.0; } })
	.uint16be('PV4Current', { formatter: (x) => { return x / 10.0; } })
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
	.seek(2)
	.int16be('BatteryPower')
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
	.uint16be('PV5Voltage', { formatter: (x) => { return x!=65535?(x/10.0):0; } })
	.uint16be('PV5Current', { formatter: (x) => { return x!=65535?(x/10.0):0; } })
	.uint16be('PV6Voltage', { formatter: (x) => { return x!=65535?(x/10.0):0; } })
	.uint16be('PV6Current', { formatter: (x) => { return x!=65535?(x/10.0):0; } })
	.seek((35336 - 35307) * 2)
	.uint16be('MPPT1Power', { formatter: (x) => { return x!=65535?x:-1; } })
	.uint16be('MPPT2Power', { formatter: (x) => { return x!=65535?x:-1; } })
	.uint16be('MPPT3Power', { formatter: (x) => { return x!=65535?x:-1; } })
	.seek((35344 - 35339) * 2)
	.uint16be('MPPT1Current', { formatter: (x) => { return x!=65535?(x/10.0):-1; } })
	.uint16be('MPPT2Current', { formatter: (x) => { return x!=65535?(x/10.0):-1; } })
	.uint16be('MPPT3Current', { formatter: (x) => { return x!=65535?(x/10.0):-1; } })
;

const ETPayloadParser_36000 = new Parser()
	.uint16be('COMMode')
	.uint16be('RSSI')
	.uint16be('ManufacturerCode')
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


async function getETSN(address) {
	try {
		modbusClient.setID(address);
		let vals = await modbusClient.readHoldingRegisters(0x88BB, 8);
		var SNStr = new String(vals.buffer);
		GWSerialNumber[address] = SNStr 
		if (options.debug) {
			console.log(SNStr);
		}
		return SNStr;
	} catch (e) {
		if (options.debug) {
			console.error("getETSN: " + e.message);
		}
		if(e.errno) {
            if(networkErrors.includes(e.errno)) {
                process.exit(-1);
            }
		}
		return null;
	}
}

const getETRegisters = async (address) => {
	try {
		modbusClient.setID(address);
		let vals = await modbusClient.readHoldingRegisters(35100, 123);
		var gwState_35100 = ETPayloadParser_35100.parse(vals.buffer);

		vals = await modbusClient.readHoldingRegisters(35304, 44);
		var gwState_35304 = ETPayloadParser_35304.parse(vals.buffer);

		vals = await modbusClient.readHoldingRegisters(36000, 38);
		var gwState_36000 = ETPayloadParser_36000.parse(vals.buffer);

		vals = await modbusClient.readHoldingRegisters(37000, 48);
		var gwState_37000 = ETPayloadParser_37000.parse(vals.buffer);

		vals = await modbusClient.readHoldingRegisters(45222, 22);
		var gwState_45222 = ETPayloadParser_45222.parse(vals.buffer);

		var gwState = {};
		Object.assign(gwState, gwState_35100, gwState_36000, gwState_35304, gwState_37000, gwState_45222);

		gwState.PV1Power = parseInt(gwState.PV1Voltage * gwState.PV1Current);
		gwState.PV2Power = parseInt(gwState.PV2Voltage * gwState.PV2Current);
		gwState.PV3Power = parseInt(gwState.PV3Voltage * gwState.PV3Current);
		gwState.PV4Power = parseInt(gwState.PV4Voltage * gwState.PV4Current);
		gwState.PV5Power = parseInt(gwState.PV5Voltage * gwState.PV5Current);
		gwState.PV6Power = parseInt(gwState.PV6Voltage * gwState.PV6Current);

		await sendMqtt(GWSerialNumber[address], gwState);
		if (options.debug) {
			console.log(util.inspect(gwState));
		}
		return gwState;
	} catch (e) {
		if (options.debug) {
			console.error("getETRegisters: " + e.message);
		}
		if(e.errno) {
            if(networkErrors.includes(e.errno)) {
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

async function getDTSN (address) {
	try {
		modbusClient.setID(address);
		let vals = await modbusClient.readHoldingRegisters(0x200, 8);
		var SNStr = new String(vals.buffer);
		GWSerialNumber[address] = SNStr;
		if (options.debug) {
			console.log(SNStr);
		}
		return SNStr;
	} catch (e) {
		if (options.debug) {
			console.error("getDTSN: " + e.message);
		}
		if(e.errno) {
            if(networkErrors.includes(e.errno)) {
                process.exit(-1);
            }
		}
		return null;
	}
}

async function getDTRegisters (address) {
	try {
		modbusClient.setID(address);
		let vals = await modbusClient.readHoldingRegisters(0x300, 0x21);
		var gwState = DTPayloadParser.parse(vals.buffer);
		gwState.PV1Power = parseInt(gwState.PV1Voltage * gwState.PV1Current);
		gwState.PV2Power = parseInt(gwState.PV2Voltage * gwState.PV2Current);
		await sendMqtt(GWSerialNumber[address], gwState);
		if (options.debug) {
			console.log(util.inspect(gwState));
		}
		return gwState;
	} catch (e) {
		if (options.debug) {
			console.error("getDTRegisters: " + e.message);
		}
		if(e.errno) {
            if(networkErrors.includes(e.errno)) {
                process.exit(-1);
            }
		}
		return null;
	}
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

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
				if (!GWSerialNumber[address]) {
					if (type == 'DT') {
						await getDTSN(address);
					} else {
						await getETSN(address);
					}
				}
			});
			await sleep(100);
			await mutex.runExclusive(async () => {
				if (GWSerialNumber[address]) {
					if (type == 'DT') {
						await getDTRegisters(address);
					} else {
						await getETRegisters(address);
					}
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


